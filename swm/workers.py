import json
import math
import signal
import time
import traceback
import uuid

import redis

from .events import TaskCompleteEvent, TaskErrorEvent
from .population.controls.static import StaticControl
from .population.spawners.static import StaticSpawner
from .tasks import BaseTask

__all__ = ['BaseWorker']


class BaseWorker:
    """
    A base class for defining worker behaviour.
    """

    def __init__(
        self,
        conn,
        task_cls,
        broadcast_channel='swm_events',
        max_spawn_time=0,
        max_status_interval=600,
        sleep_interval=1,
        idle_lifespan=0,
        population_control=None,
        population_spawner=None,
        population_lock_cool_off=60
    ):

        # The redis connection (instance) to use for communicating with other
        # workers, servers and the monitor/manager.
        self._conn = conn

        # The class of tasks the worker handles
        self._task_cls = task_cls

        # The channel that task events (e.g complete, error) will be broadcast
        # over.
        self._broadcast_channel = broadcast_channel

        # The maximum amount of time allocated to a worker to spawn new
        # workers. If set to 0 the worker will wait forever.
        self._max_spawn_time = max_spawn_time

        # The maximum number of seconds between status updates before the
        # worker is considered to have died.
        self._max_status_interval = max_status_interval

        # The interval between an idle worker checking between tasks
        self._sleep_interval = sleep_interval

        # The workers unique Id (acquired when started)
        self._id = None

        # The maximum period of time a worker can be idle for before it
        # attempts to shut itself down. If 0 the worker will never attempt to
        # shut itself down.
        self._idle_lifespan = idle_lifespan

        # The time the worker was last active (was started or completed a
        # task).
        self._idle_since = None

        # The population control used to regulate the size of the worker
        # population.
        self._population_control = population_control or StaticControl()

        # The population spawner used to spawn new workers
        self._population_spawner = population_spawner or StaticSpawner()

        # The period of wait before the population control lock is released if
        # an error occurred while spawning new workers.
        self._population_lock_cool_off = population_lock_cool_off

    def __str__(self):
        return self._id

    @property
    def id(self):
        return self._id

    def do_task(self, task):
        """Do the given task"""

    def get_tasks(self):
        """Return a list of all relevant tasks for the worker"""

        task_cls_list = self._task_cls
        if isinstance(task_cls_list, BaseTask):
            task_cls_list = [task_cls_list]

        tasks_map = {}

        for task_cls in task_cls_list:

            task_prefix = task_cls.get_id_prefix()
            task_ids = set([
                id for id in self._conn.scan_iter(f'{task_prefix}:*')
            ])

            if not task_ids:
                continue

            tasks = [task_cls.loads(t) for t in self._conn.mget(task_ids)]
            tasks_map.update(zip(task_ids, tasks))

        return tasks_map

    def get_workers(self):
        """Return a list of registered workers"""
        return set([
            id for id in self._conn.scan_iter(f'{self.get_id_prefix()}:*')
        ])

    def start(self):
        """Start the worker"""

        # Set a unique Id for the worker
        self._id = f'{self.get_id_prefix()}:{uuid.uuid4()}'

        # Register the worker
        self._conn.setex(self._id, self._max_status_interval, 'idle')

        # Start the main loop
        try:
            self._idle_since = time.time()
            self._loop()

        except KeyboardInterrupt:
            pass

        finally:
            self.shut_down()

    def shut_down(self):
        """Shutdown the worker"""

        # Unregister the worker
        self._conn.delete(self._id)

    # Event broadcasting

    def on_complete(self, task_id, data):
        """Broadcast a task completed event"""
        event = TaskCompleteEvent(task_id, data)
        self._conn.publish(self._broadcast_channel, event.dumps())

    def on_error(self, task_id, error):
        """Broadcast a task error event"""
        event = TaskErrorEvent(task_id, str(error))
        self._conn.publish(self._broadcast_channel, event.dumps())

    # Private

    def _loop(self):
        """The application loop"""

        while True:

            # Check to see if the shutdown flag is present for the worker
            # class.
            if self._conn.get(self.get_shutdown_key()):
                return self.shut_down()

            # Get a lists of workers and the tasks to complete
            workers = self.get_workers()
            tasks = self.get_tasks()

            # If the worker is no longer registered then the main loop should
            # be exited.
            if self._id not in workers:
                return

            # Population check (growth and culling)
            population_change = self._population_control.population_change(
                workers,
                tasks
            )
            population_lock_key = self.get_population_lock_key()
            time_idle = time.time() - self._idle_since

            self._conn.delete(population_lock_key)

            if population_change > 0:

                # Attempt to get a population lock so we can spawn new workers
                if self._conn.setnx(population_lock_key, self.id):
                    try:
                        self._population_spawner.spawn(population_change)

                        # Wait until the new workers have spawned
                        spawned_at = time.time()
                        while True:
                            added_workers = self.get_workers() - workers

                            if len(added_workers) >= population_change:
                                break

                            spawning = time.time() - spawned_at
                            if spawning > (self._max_spawn_time or math.inf):
                                break

                            time.sleep(1)

                        self._conn.delete(population_lock_key)

                    except Exception as e:

                        # Report the error but allow the worker to continue to
                        # run.
                        print(traceback.format_exc())
                        print(e)

                        # Delay removing the population lock to ensure we
                        # don't end up in a race condition between
                        self._conn.expire(
                            population_lock_key,
                            self._population_lock_cool_off
                        )
                        time.sleep(self._population_lock_cool_off)

            elif population_change < 0 \
                    and time_idle > (self._idle_lifespan or math.inf):

                # Attempt to get a population lock so we can shut this worker
                # down.
                if self._conn.setnx(population_lock_key, self.id):
                    try:
                        return self.shut_down()

                    finally:
                        self._conn.delete(population_lock_key)

            # Find any tasks that are currently assigned to workers that are
            # no longer registered and convert them to pending tasks.
            for task in tasks.values():

                # Check for any task that is assigned to a non-existent worker
                if task.assigned_to and task.assigned_to not in workers:

                    if self._conn.setnx(task.reclaim_key, self._id):

                        # Clear the existing lock for the task
                        self._conn.expire(task.reclaim_key, 10)
                        self._conn.delete(task.lock_key)

                        # Unassign the task so it will be handled by this
                        # worker.
                        task.unassign()

                # Check if the task is pending
                if not task.assigned_to:

                    if self._conn.setnx(task.lock_key, self.id):

                        # Assign the task to this worker
                        task.assign_to(self._id)
                        self._conn.set(task._id, task.dumps())

                        # Update the workers status to busy
                        self._conn.setex(
                            self._id,
                            self._max_status_interval,
                            'busy'
                        )

                        # Process the task
                        try:
                            event_data = self.do_task(task)
                            self.on_complete(task.id, event_data)

                        except Exception as e:
                            self.on_error(task.id, e)

                        finally:

                            # Delete the task and associated lock
                            self._conn.delete(task.id)
                            self._conn.delete(task.lock_key)

                            # Record the time at which the worker became idle
                            self._idle_since = time.time()

            # Update the workers status to idle
            self._conn.setex(self._id, self._max_status_interval, 'idle')

            time.sleep(self._sleep_interval)

    # Class methods

    @classmethod
    def get_id_prefix(cls):
        """Return a prefix applied to worker Ids"""
        return 'swm_worker'

    @classmethod
    def get_population_lock_key(cls):
        """
        Return the lock key for this class or workers to use when managing the
        population.
        """
        return f'{cls.get_id_prefix()}_population_contol'

    @classmethod
    def get_shutdown_key(cls):
        """Return the key used to flag to workers that they should shutdown"""
        return f'{cls.get_id_prefix()}_shutdown'

import math
import os
import psutil

from . import BaseControl


class OnDemandControl(BaseControl):
    """
    Maintain the current population.
    """

    def __init__(self,
        min_population=1,
        buffer_size=1,
        max_population=0,
        max_node_population=None,
        max_spawn_rate=0
    ):

        # The minimum population of workers required
        self._min_population = min_population

        # The number of standby (idle) workers the population should support.
        # These act as a buffer to help ensure that a workers are instantly
        # available for incoming tasks.
        self._buffer_size = buffer_size

        # The maximum population of workers allowed (if set to 0 there is no
        # limit).
        self._max_population = max_population

        # The maximum population of workers allowed on this node (if set to
        # 0 there is no limit, if the value is left as None then we default
        # to the number of available cores).
        self._max_node_population = max_node_population
        if self._max_node_population is None:
            self._max_node_population = max(1, len(os.sched_getaffinity(0)))

        # The maximum number of workers to spawn in a spawning
        self._max_spawn_rate = max_spawn_rate

    def population_change(self, workers, node_workers, tasks):

        # Determine the number of unassigned tasks
        assigned = {}
        for task in tasks.values():
            if task.assigned_to and task.assigned_to in workers:
                assigned[task.assigned_to] = task

        unassigned_tasks = len(tasks) - len(assigned)

        # Determine the number of idle workers
        idle_workers = len([w for w in workers if w not in assigned])

        # Determine the current population and buffer size
        current_population = len(workers)
        current_buffer = idle_workers - unassigned_tasks

        # Determine the required population size
        required_population = min(
            self._max_population or math.inf,
            max(self._min_population, len(tasks) + self._buffer_size)
        )

        # Calculate the required change in population
        required_change = min(
            self._max_spawn_rate or math.inf,
            required_population - current_population
        )

        # Determine the maximum node population
        max_node_population = self._max_node_population or math.inf

        # Apply any limits to the maximum node population
        required_change = min(
            max(0, max_node_population - len(node_workers)),
            required_change
        )

        return required_change

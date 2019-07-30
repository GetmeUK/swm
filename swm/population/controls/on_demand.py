import math
import os

from . import BaseControl


class OnDemandControl(BaseControl):
    """
    Maintain the current population.
    """

    def __init__(self,
        min_population=1,
        buffer_size=1,
        max_population=0,
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

        # The maximum number of workers to spawn in a spawning
        self._max_spawn_rate = max_spawn_rate

    def population_change(self, workers, tasks):

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
            max(self._min_population, unassigned_tasks + self._buffer_size)
        )

        # Return the population change required
        return min(
            self._max_spawn_rate or math.inf,
            required_population - current_population
        )

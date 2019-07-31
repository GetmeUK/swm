

class BaseControl:
    """
    The base class for defining population controls for workers.
    """

    def population_change(self, workers, node_workers, tasks):
        """
        Return the recommended change in population to handle the current task
        workload. A negative number will allow workers to shut themselves down
        to reduce the population, a postive value will encorage workers to
        spawn new workers to cope with the current workload.

        The workers, node workers and tasks are provided by the worker based
        on it's last query for registered workers and tasks.
        """
        raise NotImplemented()


from . import on_demand
from . import static

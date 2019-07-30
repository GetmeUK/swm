

class BaseSpawner:
    """
    The base class for defining spawning behaviour for a worker.
    """

    def spawn(self, amount):
        """Spawn the given number of new workers"""
        raise NotImplemented()


from . import aws
from . import local
from . import static

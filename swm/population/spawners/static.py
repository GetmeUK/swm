import os

from . import BaseSpawner


class StaticSpawner(BaseSpawner):
    """
    Do not spawn workers.
    """

    def spawn(self, amount):
        pass

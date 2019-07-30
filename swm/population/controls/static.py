import os

from . import BaseControl


class StaticControl(BaseControl):
    """
    Maintain the current population.
    """

    def population_change(self, workers, tasks):
        return 0

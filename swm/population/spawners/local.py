import os
import subprocess

from . import BaseSpawner


class LocalSpawner(BaseSpawner):
    """
    Spawn a new worker on the local system.
    """

    def __init__(self, args):

        # The args to use to spawn a new worker
        self._args = args

    def spawn(self, amount):
        for w in range(0, amount):
            subprocess.Popen(self._args, preexec_fn=os.setpgrp)

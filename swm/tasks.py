import datetime
import json
import time
import uuid

import redis

__all__ = ['BaseTask']


class BaseTask:
    """
    A base class for defining tasks.
    """

    def __init__(self):

        # Set a unique Id for the task
        self._id = f'{self.get_id_prefix()}:{uuid.uuid4()}'

        # Timestamp the task
        self._timestamp = time.time_ns()

        # The Id of the worker the task is assigned to
        self._assigned_to = None

    def __str__(self):
        return self._id

    @property
    def assigned_to(self):
        return self._assigned_to

    @property
    def id(self):
        return self._id

    @property
    def lock_key(self):
        return f'lock:{self._id}'

    @property
    def reclaim_key(self):
        return f'lock:{self._id}'

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def timestamp_dt(self):
        return datetime.datetime.utcfromtimestamp(self._timestamp / (10 ** 9))

    def assign_to(self, worker_id):
        """Assign the task to a worker"""
        self._assigned_to = worker_id

    def dumps(self):
        """Return a JSON string for the task"""
        return json.dumps(self.to_json_type())

    def to_json_type(self):
        """Return a dictionary containing a JSON safe version of the task"""
        return {
            'id': self._id,
            'assigned_to': self._assigned_to,
            'timestamp': self._timestamp
        }

    def unassign(self):
        """Assign the task to a worker"""
        self._assigned_to = None

    @classmethod
    def from_json_type(cls, data):
        """Convert a dictionary of JSON safe data to a task"""

        assigned_to = data.pop('assigned_to')
        id = data.pop('id')
        timestamp = data.pop('timestamp')

        task = cls(**data)

        task._assigned_to = assigned_to
        task._id = id
        task._timestamp = timestamp

        return task

    @classmethod
    def get_id_prefix(cls):
        """Return a prefix applied to task Ids"""
        return 'swm_task'

    @classmethod
    def loads(cls, s):
        """Load a JSON string as a task"""
        return cls.from_json_type(json.loads(s))

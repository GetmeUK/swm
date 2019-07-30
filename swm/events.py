import json

__all__ = [
    'BaseEvent',
    'TaskCompleteEvent',
    'TaskErrorEvent'
]


class BaseEvent:
    """
    A base event class.
    """

    def __init__(self, type):

        # The type of event
        self._type = type

    def __str__(self):
        return self._type

    @property
    def type(self):
        return self._type

    def dumps(self):
        """Return a JSON string for the event"""
        return json.dumps(self.to_json_type())

    def to_json_type(self):
        """Return a dictionary containing a JSON safe version of the event"""
        return {'type': self._type}

    @classmethod
    def from_json_type(cls, data):
        """Convert a dictionary of JSON safe data to an event"""
        return cls(**data)

    @classmethod
    def loads(cls):
        """Load a JSON string as an event"""
        return cls.from_json_type(json.loads(s))


class TaskCompleteEvent(BaseEvent):
    """
    An event published when a task is completed.
    """

    def __init__(self, task_id, data):
        super().__init__('task_complete')

        # The Id of task that was completed
        self._task_id = task_id

        # The data associated with the event (typically this forms the body
        # of the response to the caller).
        self._data = data

    @property
    def data(self):
        return self._data

    @property
    def task_id(self):
        return self._task_id

    def to_json_type(self):
        """Return a dictionary containing a JSON safe version of the event"""
        return {
            'data': self._data,
            'task_id': self.task_id,
            'type': self._type
        }

    @classmethod
    def from_json_type(cls, data):
        """Convert a dictionary of JSON safe data to an event"""
        data.pop('type')
        return cls(**data)


class TaskErrorEvent(BaseEvent):
    """
    An event published when an error occurs attempting to do task.
    """

    def __init__(self, task_id, reason):
        super().__init__('task_error')

        # The Id of task that was completed
        self._task_id = task_id

        # The reason for the error
        self._reason = reason

    @property
    def reason(self):
        return self._reason

    @property
    def task_id(self):
        return self._task_id

    def to_json_type(self):
        """Return a dictionary containing a JSON safe version of the event"""
        return {
            'reason': self._reason,
            'task_id': self._task_id,
            'type': self._type
        }

    @classmethod
    def from_json_type(cls, data):
        """Convert a dictionary of JSON safe data to an event"""
        data.pop('type')
        return cls(**data)

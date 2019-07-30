import asyncio
import json

import aioredis
import tornado.locks
import tornado.web

from swm.events import TaskCompleteEvent, TaskErrorEvent

__all__ = [
    'Application',
    'RequestHandler',
    'TaskEventListener'
]


class Application(tornado.web.Application):

    @property
    def redis(self):
        return self._redis

    @property
    def redis_sub(self):
        return self._redis_sub

    @property
    def task_event_listener(self):
        return self._task_event_listener

    async def listen_for_task_events(self, conn, channel='swm_events'):
        """"""
        self._task_event_listener = TaskEventListener()
        await self._task_event_listener.listen(conn, channel)


class RequestHandler(tornado.web.RequestHandler):

    def initialize(self):
        self.wait_future = None

    @property
    def redis(self):
        return self.application.redis

    @property
    def task_event_listener(self):
        return self.application.task_event_listener

    async def add_task_and_wait(self, task):
        """Add a task to the heap and wait for an event"""
        await self.redis.set(task.id, json.dumps(task.to_json_type()))

        event = None
        while not event:
            self.wait_future = self.task_event_listener.wait()
            
            try:
                await self.wait_future
            except asyncio.CancelledError:
                return
            
            event = self.task_event_listener.get_event(task.id) 

        if self.request.connection.stream.closed():
            return

        return event

    async def add_task_and_forget(self, task):
        """Add a task to the heap and return (e.g don't wait)"""
        await self.redis.set(task.id, json.dumps(task.to_json_type()))

    def on_connection_close(self):
        # Cancel any wait future
        if self.wait_future:
            self.wait_future.cancel()


class TaskEventListener:
    """
    The `TaskEventListener` class provides a fire and wait mechanism for 
    request handlers posting tasks to the worker network. 
    """

    EVENT_TYPES = {
        'task_complete': TaskCompleteEvent,
        'task_error': TaskErrorEvent
    }

    def __init__(self):

        # The lock used to allow request handlers to listen for a task event 
        # from the worker network.
        self._condition = tornado.locks.Condition()

        # The last event received by the listener
        self._last_event = None

    def get_event(self, task_id):
        """Return the last event if it matches the given task Id"""
        if self._last_event:
            if self._last_event.task_id == task_id:
                return self._last_event

    async def listen(self, conn, channel='swm_events'):
        """Listen for task events"""
        asyncio.ensure_future(
            self._receive((await conn.subscribe(channel))[0])
        )

    def wait(self):
        """Wait for a task event"""
        return self._condition.wait()

    async def _receive(self, channel):
        """Handle receiving an event (message) from redis"""

        while await channel.wait_message():
            data = await channel.get_json()

            # Check we received a know event type
            event_cls = self.EVENT_TYPES.get(data.get('type'))
            if event_cls:

                # Store the event
                self._last_event = event_cls.from_json_type(data)

                # Notify all listeners that a new event has been received
                self._condition.notify_all()

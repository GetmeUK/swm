import asyncio
import json
import time

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

    def __init__(self, received_lifespan=60):

        # The lock used to allow request handlers to listen for a task event
        # from the worker network.
        self._condition = tornado.locks.Condition()

        # A table of events received by the listener
        self._received_events = {}

        # The lifespan of a received event (during which it must be collected)
        # before it is purged from the table of received events.
        self._received_lifespan = received_lifespan

    def _purge_expired_received_events(self):
        """Purge received events that have expired from the table"""

        for task_id, event_and_timestamp \
                in list(self._received_events.items()):

            event, timestamp = event_and_timestamp

            if (time.time() - timestamp) > self._received_lifespan:
                self._received_events.pop(task_id, None)

    def get_event(self, task_id):
        """
        Return a received event if it matches the given task Id (the event can
        only be returned once, subsequent calls will return `None`).
        """
        if task_id in self._received_events:
            event_and_timestamp = self._received_events.pop(task_id, None)
            if event_and_timestamp:
                return event_and_timestamp[0]

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
                event = event_cls.from_json_type(data)
                self._received_events[event.task_id] = (
                    event,
                    time.time()
                )

                # Notify all listeners that a new event has been received
                self._condition.notify_all()

            # Clean up
            self._purge_expired_received_events()

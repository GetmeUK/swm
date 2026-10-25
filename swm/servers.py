
import asyncio
import json
import logging
import time

import tornado.locks
import tornado.web

from swm.events import TaskCompleteEvent, TaskErrorEvent

__all__ = [
    'Application',
    'RequestHandler',
    'TaskEventListener'
]

logger = logging.getLogger(__name__)


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
        """Start listening for task events."""
        self._task_event_listener = TaskEventListener()
        await self._task_event_listener.listen(conn, channel)


class RequestHandler(tornado.web.RequestHandler):

    @property
    def redis(self):
        return self.application.redis

    @property
    def task_event_listener(self):
        return self.application.task_event_listener

    async def add_task_and_wait(self, task):
        """Add a task to Redis and wait for its completion event."""

        await self.redis.set(
            task.id,
            json.dumps(task.to_json_type())
        )

        self.wait_future = None

        while True:
            # Check for events received before we started waiting
            event = self.task_event_listener.get_event(task.id)

            if event is not None:
                break

            # Register the waiter before checking again
            self.wait_future = self.task_event_listener.wait()

            # Check again to avoid missing an event
            event = self.task_event_listener.get_event(task.id)

            if event is not None:
                self.wait_future.cancel()
                self.wait_future = None
                break

            try:
                await self.wait_future
            except asyncio.CancelledError:
                return
            finally:
                self.wait_future = None

        if self.request.connection.stream.closed():
            return

        return event

    async def add_task_and_forget(self, task):
        """Add a task to Redis without waiting for its completion."""
        await self.redis.set(
            task.id,
            json.dumps(task.to_json_type())
        )

    def on_connection_close(self):
        """Cancel any outstanding wait when the client disconnects."""
        if getattr(self, 'wait_future', None):
            self.wait_future.cancel()


class TaskEventListener:
    """
    Receives task events from Redis PubSub and notifies waiting
    Tornado request handlers.

    Compatible with redis.asyncio.
    """

    EVENT_TYPES = {
        'task_complete': TaskCompleteEvent,
        'task_error': TaskErrorEvent
    }

    def __init__(self, received_lifespan=60):

        self._condition = tornado.locks.Condition()

        # Received events waiting to be collected
        self._received_events = {}
        self._received_lifespan = received_lifespan

        # Redis connection and PubSub channel
        self._conn = None
        self._channel = None

        # Background listener task
        self._listener_task = None

    def _purge_expired_received_events(self):
        """Remove cached events that have expired."""

        now = time.time()

        for task_id, (event, timestamp) in list(
            self._received_events.items()
        ):
            if (now - timestamp) > self._received_lifespan:
                self._received_events.pop(task_id, None)

    def get_event(self, task_id):
        """
        Return and remove a received task event.

        Each event can only be collected once.
        """
        self._purge_expired_received_events()

        event_and_timestamp = self._received_events.pop(task_id, None)

        if event_and_timestamp:
            return event_and_timestamp[0]

        return None

    def wait(self):
        """Wait until a new task event is received."""
        return self._condition.wait()

    async def listen(self, conn, channel='swm_events'):
        """
        Start the Redis PubSub listener.

        Wait for the initial subscription to succeed so that
        task events aren't missed during application startup.
        """

        self._conn = conn
        self._channel = channel

        # Stop an existing listener before starting another
        await self.stop()

        ready = asyncio.get_running_loop().create_future()

        self._listener_task = asyncio.create_task(
            self._listen_loop(ready)
        )

        await ready

    async def _listen_loop(self, ready):
        """
        Maintain the Redis subscription and reconnect on failure.

        Uses exponential backoff when reconnection is needed.
        """

        retry_delay = 1

        while True:
            try:
                async with self._conn.pubsub() as pubsub:

                    await pubsub.subscribe(self._channel)

                    logger.info(
                        "Subscribed to Redis channel: %s",
                        self._channel
                    )

                    retry_delay = 1

                    if not ready.done():
                        ready.set_result(True)

                    await self._receive(pubsub)

                    logger.warning(
                        "Redis PubSub listener ended; reconnecting"
                    )

            except asyncio.CancelledError:
                if not ready.done():
                    ready.cancel()
                raise

            except Exception:
                logger.exception(
                    "Redis PubSub listener failed; retrying in %s seconds",
                    retry_delay
                )

            await asyncio.sleep(retry_delay)

            retry_delay = min(retry_delay * 2, 60)

    async def _receive(self, pubsub):
        """Receive and process task events from Redis PubSub."""

        async for message in pubsub.listen():

            # Ignore subscribe/unsubscribe notifications
            if message.get('type') != 'message':
                continue

            try:
                # Redis returns bytes by default
                payload = message.get('data')

                if isinstance(payload, bytes):
                    payload = payload.decode('utf-8')

                data = json.loads(payload)

                # Find the matching event class
                event_cls = self.EVENT_TYPES.get(data.get('type'))

                if event_cls is None:
                    logger.warning(
                        "Unknown task event type: %s",
                        data.get('type')
                    )
                    continue

                # Deserialize the event
                event = event_cls.from_json_type(data)

                # Store it for waiting request handlers
                self._received_events[event.task_id] = (
                    event,
                    time.time()
                )

                # Wake all waiting request handlers
                self._condition.notify_all()

            except (ValueError, TypeError, KeyError, AttributeError):
                logger.exception(
                    "Unable to process Redis task event"
                )

            finally:
                self._purge_expired_received_events()

    async def stop(self):
        """Stop the background listener and close its subscription."""

        task = self._listener_task
        self._listener_task = None

        if task is not None:
            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass

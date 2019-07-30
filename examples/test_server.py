import asyncio
import json

import aioredis
import swm

from shared import MyTask, MyWorker


class MyTaskHandler(swm.servers.RequestHandler):

    async def get(self):

        task = MyTask(self.get_argument('content', 'bar'))
        event = await self.add_task_and_wait(task)

        self.write(event.to_json_type())


class Application(swm.servers.Application):

    def __init__(self, *args, **kwargs):

        super().__init__(
            [('/', MyTaskHandler)],
            *args,
            **kwargs
        )

        loop = asyncio.get_event_loop()

        # Set up redis connections
        self._redis = loop.run_until_complete(
            aioredis.create_redis(('localhost', 6379), loop=loop)
        )

        self._redis_sub = loop.run_until_complete(
            aioredis.create_redis(('localhost', 6379), loop=loop)
        )

        # Set up the event listener
        loop.run_until_complete(self.listen_for_task_events(self._redis_sub))



if __name__ == '__main__':

    app = Application(debug=True)
    app.listen(5000)
    asyncio.get_event_loop().run_forever()

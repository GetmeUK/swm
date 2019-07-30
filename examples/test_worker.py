import json

import redis
from swm.tasks import BaseTask
from swm.workers import BaseWorker

from shared import MyTask, MyWorker


if __name__ == '__main__':
    worker = MyWorker(redis.Redis(), MyTask)
    worker.start()

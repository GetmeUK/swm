import json

import redis
from swm.monitors import get_tasks, get_workers

from shared import MyTask, MyWorker


if __name__ == '__main__':

    conn = redis.Redis()

    print(get_tasks(conn, MyTask))
    print(get_workers(conn, MyWorker))

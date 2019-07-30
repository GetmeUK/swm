"""
Utils for monitoring workers and tasks.
"""

import operator
import time

__all__ = [
    'get_tasks',
    'get_workers',
    'shutdown_workers'
]


def get_tasks(conn, *task_cls_list):
    """Return a list of incomplete tasks in the heap"""

    tasks = []
    for task_cls in task_cls_list:

        task_prefix = task_cls.get_id_prefix()
        task_ids = set([id for id in conn.scan_iter(f'{task_prefix}:*')])

        if not task_ids:
            continue

        tasks.extend([task_cls.loads(t) for t in conn.mget(task_ids)])

    return sorted(tasks, key=operator.attrgetter('timestamp'))

def get_workers(conn, worker_cls):
    """Return a list of active workers [(worker_id, status)]"""
    worker_prefix = worker_cls.get_id_prefix()
    worker_ids = set([id for id in conn.scan_iter(f'{worker_prefix}:*')])
    return sorted(
        zip(worker_ids, conn.mget(worker_ids)),
        key=operator.itemgetter(0)
    )

def shutdown_workers(conn, worker_cls):
    """Shutdown all workers for the given worker class"""

    try:
        conn.set(worker_cls.get_shutdown_key(), 'shutdown')

        while True:
            workers = list(conn.scan_iter(f'{worker_cls.get_id_prefix()}:*'))
            if len(workers) == 0:
                break

            time.sleep(1)

    finally:
        conn.delete(worker_cls.get_shutdown_key())

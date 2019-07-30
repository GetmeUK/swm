import time

from swm.tasks import BaseTask
from swm.workers import BaseWorker


class MyWorker(BaseWorker):

    def do_task(self, task):
        print('>>>', task.content)

        time.sleep(5)

        return {'content': task.content}


class MyTask(BaseTask):

    def __init__(self, content):
        super().__init__()

        # The content the worker should print
        self.content = content

    def to_json_type(self):
        data = super().to_json_type()
        data['content'] = self.content
        return data

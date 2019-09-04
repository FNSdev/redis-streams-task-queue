import json
import logging
from functools import wraps

from redis_streams_task_queue.exceptions import TaskDoesNotExistException, QueueIsAlreadyRegisteredException

logger = logging.getLogger(__name__)


class TaskLibrary:
    def __init__(self):
        self._tasks = {}
        self._queues = []

    def register_queue(self, queue):
        if queue in self._queues:
            raise QueueIsAlreadyRegisteredException(f'Queue with name {queue} is already registered')
        self._queues.append(queue)

    def task(self, func):
        self._tasks[func.__name__] = func

        @wraps(func)
        async def wrapper(*args, **kwargs):
            message = self.serialize_task(func, *args, **kwargs)
            for queue in self._queues:
                message_id = await queue.send_message(message)

                # logger.debug('Sending message "%s" in a queue "%s"' % (message, queue))

        return wrapper

    # TODO do not dump
    # TODO try to convert types like datetime to str if possible
    def serialize_task(self, task, *args, **kwargs):
        return json.dumps(
            {
                'task': task.__name__,
                'args': args,
                'kwargs': kwargs
            }
        )

    def deserialize_task(self, task):
        task = json.loads(task)

        task_name = task['task']
        args = task['args']
        kwargs = task['kwargs']

        if task_name not in self._tasks:
            raise TaskDoesNotExistException(f'Task with name "{task_name}" is not registered')
        return self._tasks[task_name], args, kwargs

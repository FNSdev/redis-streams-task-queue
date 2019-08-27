import logging
import json
from functools import wraps

import aioredis
from aioredis.errors import ReplyError
from aioredis.util import wait_ok

from redis_streams_task_queue.exceptions import TaskDoesNotExistException

logger = logging.getLogger(__name__)


class TaskQueue:
    def __init__(self, redis_address, stream_key='tasks', consumer_group_name='consumers'):
        self._client = None
        self._redis_address = redis_address

        self._stream_key = stream_key
        self._consumer_group_name = consumer_group_name

        self._tasks = {}

    async def connect(self):
        self._client = await aioredis.create_redis(self._redis_address)
        await self._create_consumer_group()

    def disconnect(self):
        self._client.close()

    def task(self, func):
        self._tasks[func.__name__] = func

        @wraps(func)
        async def wrapper(*args, **kwargs):
            message = self._serialize_task(func, *args, **kwargs)
            await self._send_message(message)

            logger.debug('Sending message "%s"' % message)

        return wrapper

    def _serialize_task(self, task, *args, **kwargs):
        return json.dumps(
            {
                'task': task.__name__,
                'args': args,
                'kwargs': kwargs
            }
        )

    def _deserialize_task(self, task):
        task = json.loads(task)

        task_name = task['task']
        args = task['args']
        kwargs = task['kwargs']

        if task_name not in self._tasks:
            raise TaskDoesNotExistException(f'Task with name "{task_name}" is not registered')
        return self._tasks[task_name], args, kwargs

    async def _create_consumer_group(self):
        try:
            fut = self._client.execute(
                b'XGROUP',
                b'CREATE',
                self._stream_key,
                self._consumer_group_name,
                b'$',
                b'MKSTREAM',
            )
            await wait_ok(fut)

            logger.debug(
                'Consumer group "%s" was created for stream "%s"' % (self._consumer_group_name, self._stream_key)
            )
        except ReplyError as e:
            if str(e) == 'BUSYGROUP Consumer Group name already exists':
                pass
            else:
                raise e

    async def _send_message(self, message):
        await self._client.xadd(self._stream_key, {'message': message})

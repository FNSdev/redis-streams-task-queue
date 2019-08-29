import logging
import json
from functools import wraps

import aioredis
from aioredis.errors import ReplyError
from aioredis.util import wait_ok

from redis_streams_task_queue.exceptions import TaskDoesNotExistException

logger = logging.getLogger(__name__)


class Queue:
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

    async def send_message(self, message):
        await self._client.xadd(self._stream_key, {'message': message})

    def __str__(self):
        return f'{self._stream_key}-{hash(self)}'

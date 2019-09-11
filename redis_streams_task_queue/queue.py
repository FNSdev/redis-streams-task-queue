import logging

import aioredis

from redis_streams_task_queue.utils import create_consumer_group

logger = logging.getLogger(__name__)


class Queue:
    def __init__(self, redis_address, stream_key, consumer_group_name):
        self._client = None
        self._redis_address = redis_address

        self._stream_key = stream_key
        self._consumer_group_name = consumer_group_name

        self._tasks = {}

    async def connect(self):
        self._client = await aioredis.create_redis(self._redis_address)
        await create_consumer_group(self._client, self._stream_key, self._consumer_group_name)

    def disconnect(self):
        self._client.close()

    async def send_message(self, message):
        return await self._client.xadd(self._stream_key, {'message': message})

    def __str__(self):
        return f'{self._stream_key}-{hash(self)}'

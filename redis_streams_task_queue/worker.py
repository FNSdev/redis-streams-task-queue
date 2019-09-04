import logging
import os
import signal
import sys

import aioredis
from aioredis.errors import ReplyError
from aioredis.util import wait_ok

from redis_streams_task_queue.utils import create_consumer_group

logger = logging.getLogger(__name__)
logger.debug('PID: %s' % os.getpid())


# TODO do something with results (store them in redis, for example)
class Worker:
    def __init__(self, consumer_name, redis_address, stream_key, consumer_group_name, task_library):
        self._stop = False

        def on_stop(sig, frame):
            self._stop = True

        signal.signal(signal.SIGTERM, on_stop)

        self._client = None
        self._redis_address = redis_address
        self._task_library = task_library
        self._stream_key = stream_key
        self._consumer_group_name = consumer_group_name
        self._consumer_name = consumer_name

    async def connect(self):
        self._client = await aioredis.create_redis(self._redis_address)
        await create_consumer_group(self._client, self._stream_key, self._consumer_group_name)

    def disconnect(self):
        self._client.close()

    async def run(self):
        await self._process_pending_messages()
        await self._process_messages()

    async def _process_messages(self):
        while True:
            if self._stop:
                self.disconnect()
                sys.exit()

            message = await self._client.xread_group(
                self._consumer_group_name,
                self._consumer_name,
                [self._stream_key],
                latest_ids=['>'],
                count=1,
            )

            message_id, task, args, kwargs = self._parse_message(message)
            logger.debug('Received message "%s"' % message_id)

            await self._process_task(message_id, task, args, kwargs)

    async def _process_pending_messages(self):
        """
        In our case, only one message can be pending for each worker at the same time.
        If worker hasn`t acknowledged a message, it won`t receive new messages, until pending message will be
        processed and acknowledged by this worker.
        """

        while True:
            messages = await self._client.xpending(
                self._stream_key,
                self._consumer_group_name,
                start='-',
                stop='+',
                count=1,
                consumer=self._consumer_name,
            )

            if not messages:
                break

            # Get details
            messages = await self._client.xrange(self._stream_key, messages[0][0], messages[-1][0])

            for message in messages:
                if self._stop:
                    self.disconnect()
                    sys.exit()

                message_id, task, args, kwargs = self._parse_pending_message(message)
                logger.debug('Received pending message "%s"' % message_id)

                await self._process_task(message_id, task, args, kwargs)

    async def _process_task(self, message_id, task, args, kwargs):
        result = await self._execute(task, args, kwargs)
        await self._client.xack(self._stream_key, self._consumer_group_name, message_id)
        await self.delete_processed_message(message_id)

    async def _execute(self, task, args, kwargs):
        result = await task(*args, **kwargs)

        logger.debug('Task "%s" with args "%s" and kwargs "%s" was executed' % (task, args, kwargs))

        return result

    def _parse_message(self, message):
        message_id, task = message[0][1], message[0][2][b'message']
        task, args, kwargs = self._task_library.deserialize_task(task)
        return message_id, task, args, kwargs

    def _parse_pending_message(self, message):
        message_id, task = message[0], message[1][b'message']
        task, args, kwargs = self._task_library.deserialize_task(task)
        return message_id, task, args, kwargs

    # TODO is it ok to have this method in Worker class or is it better to have smth like self._queue
    # The problem with self._queue is that we can potentially process messages from different queues later.
    # So we will have to store queues in dict, for example
    async def delete_processed_message(self, message_id):
        try:
            fut = self._client.execute(
                b'XDEL',
                self._stream_key,
                message_id
            )
            await wait_ok(fut)
        except ReplyError as e:
            logger.exception(e)
            raise e

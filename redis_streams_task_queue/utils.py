import logging

from aioredis.errors import ReplyError
from aioredis.util import wait_ok

logger = logging.getLogger(__name__)


async def create_consumer_group(redis_client, stream_key, consumer_group_name):
    try:
        fut = redis_client.execute(
            b'XGROUP',
            b'CREATE',
            stream_key,
            consumer_group_name,
            b'$',
            b'MKSTREAM',
        )
        await wait_ok(fut)

        logger.debug(
            'Consumer group "%s" was created for stream "%s"' % (consumer_group_name, stream_key)
        )
    except ReplyError as e:
        if str(e) == 'BUSYGROUP Consumer Group name already exists':
            pass
        else:
            raise e

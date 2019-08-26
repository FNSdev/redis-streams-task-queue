import asyncio
import os
import random
import signal
import sys

import aioredis
from aioredis.errors import ReplyError

url = 'redis://127.0.0.1:6380'
stream = 'events'
group_name = 'event_consumers'
stop = False


def on_term(sig, frame):
    global stop
    stop = True


signal.signal(signal.SIGTERM, on_term)
print(os.getpid())


async def connect():
    connection = await aioredis.create_redis(url)
    try:
        await connection.xgroup_create(stream, group_name)
    except ReplyError as e:
        # The problem with aioredis is that we can`t create a group if stream does not exist
        #
        # I would suggest to create group manually with `$ xgroup create events event_consumers $ MKSTREAM`
        # instead of this code, because it will also create an empty stream

        if str(e) == 'BUSYGROUP Consumer Group name already exists':
            pass
        else:
            raise e

    return connection


async def produce():
    connection = await connect()

    while True:
        if stop:
            connection.close()
            sys.exit()

        value_1 = random.randint(1, 100)
        value_2 = random.randint(1, 100)
        fields = {
            'event_id': value_1,
            'extra': value_2
        }

        print(f'{fields} event was added')
        await connection.xadd(stream, fields)
        await asyncio.sleep(5)


if __name__ == '__main__':
    asyncio.run(produce())

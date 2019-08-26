import asyncio
import os
import signal
import sys

import aioredis

from business_logic import process_event

url = 'redis://127.0.0.1:6380'
stream = 'events'
group_name = 'event_consumers'
stop = False
last_id = '0-0'


def on_stop(sig, frame):
    global stop
    stop = True


signal.signal(signal.SIGTERM, on_stop)
print(os.getpid())


async def connect():
    return await aioredis.create_redis(url)


async def process_pending_tasks(connection, consumer_name):
    while True:
        messages = await connection.xpending(
            stream,
            group_name,
            start='-',
            stop='+',
            count='10',  # we must to limit count
            consumer=consumer_name
        )

        print(f'Pending messages for "{worker_name}" : {messages}')
        if not messages:
            break

        # Get details
        messages = await connection.xrange(stream, messages[0][0], messages[-1][0])
        print(f'Pending messages for "{worker_name}" : {messages}')

        for message in messages:
            if stop:
                connection.close()
                sys.exit()

            message_id, event = message[0], message[1]
            await process_event(event)
            await connection.xack(stream, group_name, message_id)


async def consume(consumer_name):
    connection = await connect()

    async def process_message(msg):
        message_id, event = msg[0][1], msg[0][2]
        await process_event(event)
        await connection.xack(stream, group_name, message_id)

    # Wait until all pending tasks are processed
    await process_pending_tasks(connection, consumer_name)

    # In case some messages were not delivered to any worker, one will be processed right now,
    # others will become pending for some reason, so we need to call process_pending_tasks again

    # TODO think about how to refactor it
    message = await connection.xread_group(group_name, consumer_name, [stream], latest_ids=['>'])
    await process_message(message)
    await process_pending_tasks(connection, consumer_name)

    while True:
        if stop:
            connection.close()
            sys.exit()

        message = await connection.xread_group(group_name, consumer_name, [stream], latest_ids=['>'])
        # It actually looks like this
        # [(b'events', b'1566822226846-0', OrderedDict([(b'event_id', b'52'), (b'extra', b'25')]))]
        await process_message(message)


if __name__ == '__main__':
    worker_name = sys.argv[1]
    asyncio.run(consume(worker_name))

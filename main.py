import asyncio
import random

from redis_streams_task_queue.task_queue import TaskQueue

url = 'redis://127.0.0.1:6380'

queue = TaskQueue(url, stream_key='events', consumer_group_name='event_consumers')


@queue.task
async def add(a, b):
    # Imitates long processing
    await asyncio.sleep(random.randint(5, 10))
    s = a + b
    print(s)
    return s


async def main():
    await queue.connect()
    while True:
        a = random.randint(1, 100)
        b = random.randint(1, 100)
        await add(a, b)
        await asyncio.sleep(5)


if __name__ == '__main__':
    asyncio.run(main())

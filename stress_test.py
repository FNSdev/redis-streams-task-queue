import asyncio
import random

from redis_streams_task_queue.task_library import TaskLibrary
from redis_streams_task_queue.queue import Queue

url = 'redis://127.0.0.1:6380'

queue = Queue(redis_address=url, stream_key='events', consumer_group_name='event_consumers')
task_library = TaskLibrary()


args = [

]

kwargs = {

}

for i in range(1000):
    args.append('a very very very long string')
    kwargs[i] = 'a very very very very very very very long string'


@task_library.task
async def some_task(args, kwargs):
    await asyncio.sleep(0.05)


async def main():
    task_library.register_queue(queue)
    await queue.connect()
    for _ in range(10_000_000):
        await some_task(args, kwargs)

if __name__ == '__main__':
    asyncio.run(main())

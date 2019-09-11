import asyncio
import sys

from main import task_library
from redis_streams_task_queue.worker import Worker

url = 'redis://127.0.0.1:6380'
worker = Worker(
    consumer_name=sys.argv[1],
    redis_address=url,
    stream_key='events',
    consumer_group_name='event_consumers',
    task_library=task_library
)


async def main():
    await worker.startup()
    await worker.run()


if __name__ == '__main__':
    asyncio.run(main())

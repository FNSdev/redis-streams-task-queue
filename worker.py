import asyncio
import sys

from main import queue, task_library
from redis_streams_task_queue.worker import Worker

url = 'redis://127.0.0.1:6380'
worker = Worker(url, queue, task_library, sys.argv[1])


async def main():
    await worker.connect()
    await worker.run()


if __name__ == '__main__':
    asyncio.run(main())

import asyncio
import json

import aioredis

url = 'redis://127.0.0.1:6380'


async def process(message):
    await asyncio.sleep(0.05)
    print('Processed!')


async def main():
    redis = await aioredis.create_redis(url)

    while True:
        msg = await redis.brpop('messages')
        msg = json.loads(msg[1])
        await process(msg)


if __name__ == '__main__':
    asyncio.run(main())

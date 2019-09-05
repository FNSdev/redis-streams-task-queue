import asyncio
import json

import aioredis

url = 'redis://127.0.0.1:6380'


args = [

]

kwargs = {

}

for i in range(100):
    args.append('a very very very long string')
    kwargs[i] = 'a very very very very very very very long string'


async def main():
    redis = await aioredis.create_redis(url)
    msg = json.dumps(
        {
            'task': 'abc',
            'args': args,
            'kwargs': kwargs,
        }
    )

    for _ in range(1_000_000):
        await redis.lpush('messages', msg)


if __name__ == '__main__':
    asyncio.run(main())

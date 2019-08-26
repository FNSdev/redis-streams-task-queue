import asyncio
import random


async def process_event(event):
    """Simulates processing of event"""
    await asyncio.sleep(random.randint(5, 10))

    # event['event_id'] does not work. I don`t know if it will be a problem when inserting in the db.
    print(f'Event # {event[b"event_id"]} was processed!')

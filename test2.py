import asyncio
from time import sleep, time
import os
from concurrent.futures import ProcessPoolExecutor

num_jobs = 10
num_workers = 4
queue = asyncio.Queue()
executor = ProcessPoolExecutor(max_workers=num_jobs)
loop = asyncio.get_event_loop()

async def async_work(i):
    await asyncio.sleep(1)
    print(os.getpid(),"Task",i,"completed")

def work(index):
    tasks = [loop.create_task(async_work(i)) for i in range(num_jobs) if i%num_workers == index]
    return tasks


async def producer():
    tasks = [loop.run_in_executor(executor, asyncio.run, work) for i in range(num_workers)]
    for f in asyncio.as_completed(tasks, loop=loop):
        results = await f
        await queue.put(results)

async def consumer():
    completed = 0
    while completed < num_jobs:
        job = await queue.get()
        completed += 1

s = time()
loop.run_until_complete(asyncio.gather(producer(), consumer()))
print("duration", time() - s)
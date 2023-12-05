import asyncio
import aiohttp
import pyarrow as pa
import time

async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()

async def main():
    url = 'http://localhost:8000'
    num_requests = 4
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(url, session) for _ in range(num_requests)]
        results = await asyncio.gather(*tasks)
        return results

start_time = time.time()

buffers = asyncio.run(main())

batches = []
for i in range(len(buffers)):
  with pa.ipc.open_stream(buffers[i]) as reader:
    schema = reader.schema
    try:
      while True:
        batches.append(reader.read_next_batch())
    except StopIteration:
        pass

end_time = time.time()
execution_time = end_time - start_time

print(f"{len(buffers)} bytes received")
print(f"{sum(map(len, batches))} record batches received")
print(f"{execution_time} seconds elapsed")


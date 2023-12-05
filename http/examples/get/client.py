import urllib.request
import pyarrow as pa
import time

start_time = time.time()

with urllib.request.urlopen('http://localhost:8000') as response:
  buffer = response.read()

batches = []

with pa.ipc.open_stream(buffer) as reader:
  schema = reader.schema
  try:
    while True:
      batches.append(reader.read_next_batch())
  except StopIteration:
      pass

# or:
#with pa.ipc.open_stream(buffer) as reader:
#  schema = reader.schema
#  batches = [b for b in reader]

end_time = time.time()
execution_time = end_time - start_time

print(f"{len(buffer)} bytes received")
print(f"{len(batches)} record batches received")
print(f"{execution_time} seconds elapsed")

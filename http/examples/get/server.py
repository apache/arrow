import pyarrow as pa
from random import randbytes
from http.server import BaseHTTPRequestHandler, HTTPServer

schema = pa.schema([
    ('a', pa.int64()),
    ('b', pa.int64()),
    ('c', pa.int64()),
    ('d', pa.int64())
])

def GetPutData():
    total_records = 100000000
    length = 4096
    ncolumns = 4
    
    arrays = []
    
    for x in range(0, ncolumns):
        buffer = pa.py_buffer(randbytes(length * 8))
        arrays.append(pa.Int64Array.from_buffers(pa.int64(), length, [None, buffer], null_count=0))
    
    batch = pa.record_batch(arrays, schema)
    batches = []
    
    records_sent = 0
    while records_sent < total_records:
      if records_sent + length > total_records:
        last_length = total_records - records_sent
        batches.append(batch.slice(0, last_length))
        records_sent += last_length
      else:
        batches.append(batch)
        records_sent += length
    
    return batches
 
class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        chunk_size = int(2e9)
        chunk_splits = len(buffer) // chunk_size
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/vnd.apache.arrow.stream')
        
        #######################################################################
        # include these to enable testing JavaScript client in local browser
        self.send_header('Access-Control-Allow-Origin', 'http://localhost:8080')
        self.send_header('Access-Control-Allow-Methods', 'GET')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        #######################################################################
        
        self.end_headers()
        
        for i in range(chunk_splits):
            self.wfile.write(buffer.slice(i * chunk_size, chunk_size))
            self.wfile.flush()
        self.wfile.write(buffer.slice(chunk_splits * chunk_size))
        self.wfile.flush()

batches = GetPutData()

sink = pa.BufferOutputStream()

with pa.ipc.new_stream(sink, schema) as writer:
   for i in range(len(batches)):
      writer.write_batch(batches[i])

buffer = sink.getvalue()

server_address = ('localhost', 8000)
httpd = HTTPServer(server_address, MyServer)

print(f'Serving on {server_address[0]}:{server_address[1]}...')
httpd.serve_forever()

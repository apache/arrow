import pyarrow as pa
from random import randbytes
from http.server import BaseHTTPRequestHandler, HTTPServer
import io

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

def make_reader(schema, batches):
    return pa.RecordBatchReader.from_batches(schema, batches)

def generate_batches(schema, reader):
    with io.BytesIO() as sink, pa.ipc.new_stream(sink, schema) as writer:
        yield sink.getvalue()
        
        for batch in reader:
            sink.seek(0)
            sink.truncate(0)
            writer.write_batch(batch)
            yield sink.getvalue()
        
        sink.seek(0)
        sink.truncate(0)
        writer.close()
        yield sink.getvalue()
 
class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/vnd.apache.arrow.stream')
        
        # set these headers if testing with a local browser-based client:
        
        #self.send_header('Access-Control-Allow-Origin', 'http://localhost:8000')
        #self.send_header('Access-Control-Allow-Methods', 'GET')
        #self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        
        self.end_headers()
        
        for buffer in generate_batches(schema, make_reader(schema, batches)):
            self.wfile.write(buffer)
            self.wfile.flush()
            
            # if any record batch could be larger than 2 GB, split it
            # into chunks before passing to self.wfile.write() by 
            # replacing the two lines above with this:
            
            #chunk_size = int(2e9)
            #chunk_splits = len(buffer) // chunk_size
            #for i in range(chunk_splits):
            #    self.wfile.write(buffer[i * chunk_size:i * chunk_size + chunk_size])
            #    self.wfile.flush()
            #self.wfile.write(buffer[chunk_splits * chunk_size:])
            #self.wfile.flush()

batches = GetPutData()

server_address = ('localhost', 8000)
httpd = HTTPServer(server_address, MyServer)

print(f'Serving on {server_address[0]}:{server_address[1]}...')
httpd.serve_forever()

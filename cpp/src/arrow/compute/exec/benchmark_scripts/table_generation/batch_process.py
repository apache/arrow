import json
from datagen import *

from pyarrow import ipc
from pyarrow.ipc import RecordBatchStreamWriter
import pyarrow as pa
import pyarrow.parquet as pq


class BatchStreamStats:
    def __init__(self, filename):
        self.filename = filename
        self.num_batches = 0
        self.num_rows = 0
        self.num_bytes = 0

    def close(self):
        if filename:
            with open(filename, "w") as f:
                f.write(f"num_batches = {self.num_batches}\n"
                        f"num_rows = {self.num_rows}\n"
                        f"num_bytes = {self.num_bytes}\n"
                )

    def process(self, batch_source):
        for batch in batch_source:
            self.num_batches += 1
            self.num_rows += batch.num_rows
            self.num_bytes += batch.nbytes

class BatchStreamParquetFileWriter:
    def __init__(self, filename):
        self.filename = filename
        self.pqwriter = None

    def close(self):
        if self.pqwriter is not None:
            self.pqwriter.close()

    def process(self, batch_source):
        for batch in batch_source:
            table = pa.Table.from_batches([batch], schema=batch.schema)
            if self.pqwriter is None:
                self.pqwriter = pq.ParquetWriter(self.filename, table.schema)
            self.pqwriter.write_table(table)

class BatchStreamFeatherFileWriter:
    def __init__(self, filename):
        self.filename = filename
        self.file = None
        self.ipcwriter = None

    def close(self):
        if self.ipcwriter is not None:
            self.ipcwriter.close()
        if self.file is not None:
            self.file.close()

    def process(self, batch_source):
        if self.file is None:
            self.file = open(self.filename, "wb")
        for batch in batch_source:
            if self.ipcwriter is None:
                self.ipcwriter = ipc.new_file(self.file, batch.schema)
            self.ipcwriter.write_batch(batch)

class BatchStreamRbsWriter:
    def __init__(self, filename):
        self.filename = filename
        self.file = None
        self.rbswriter = None

    def close(self):
        if self.rbswriter is not None:
            self.rbswriter.close()
        if self.file is not None:
            self.file.close()

    def process(self, batch_source):
        if self.file is None:
            self.file = open(self.filename, "wb")
        for batch in batch_source:
            if self.rbswriter is None:
                self.rbswriter = RecordBatchStreamWriter(self.file, batch.schema)
            self.rbswriter.write_batch(batch)

def generate_data(process_type, filename, dgen, dgen_json):
    process = globals()[process_type](filename)
    dgen_kwargs = json.loads(dgen_json)
    process.process(globals()[dgen]().generate(**dgen_kwargs))
    process.close()

if __name__ == "__main__":
    import sys
    pre_args = 1
    if len(sys.argv) < pre_args + 4:
        sys.stderr.write(
            "Usage: python -m bamboo.batch_process <process-type> <filename> <generator> <generator-keywords-in-json>\n" \
            "Example: python -m bamboo.batch_process BatchStreamFeatherFileWriter marketdata.feather TableMDGenerator '{\"begin_date\":\"2020-01-01\", \"end_date\":\"2020-01-01\", \"seed\":1, \"freq\":\"1h\"}'\n"
        )
        sys.exit(1)
    process_type, filename, dgen, dgen_json = sys.argv[pre_args:pre_args+4]
    generate_data(process_type, filename, dgen, dgen_json)
    sys.exit(0)

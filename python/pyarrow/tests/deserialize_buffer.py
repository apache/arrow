import sys

import pyarrow as pa

with open(sys.argv[1], 'rb') as f:
    data = f.read()
    pa.deserialize(data)

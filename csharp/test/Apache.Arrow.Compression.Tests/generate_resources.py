# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Generates files required for tests where it's not possible to generate
them with dotnet Arrow.
"""

import pyarrow as pa
from pathlib import Path


def write_compressed_ipc_file(path: Path, compression: str, stream: bool = False):
    schema = pa.schema([
        pa.field('integers', pa.int32()),
        pa.field('floats', pa.float32()),
    ])

    integers = pa.array(range(100), type=pa.int32())
    floats = pa.array((x * 0.1 for x in range(100)), type=pa.float32())
    batch = pa.record_batch([integers, floats], schema)

    options = pa.ipc.IpcWriteOptions(compression=compression)

    with pa.OSFile(path.as_posix(), 'wb') as sink:
        if stream:
            with pa.ipc.new_stream(sink, schema, options=options) as writer:
                writer.write(batch)
        else:
            with pa.ipc.new_file(sink, schema, options=options) as writer:
                writer.write(batch)


if __name__ == '__main__':
    resource_dir = Path(__file__).resolve().parent / 'Resources'

    write_compressed_ipc_file(resource_dir / 'ipc_lz4_compression.arrow', 'lz4')
    write_compressed_ipc_file(resource_dir / 'ipc_lz4_compression.arrow_stream', 'lz4', stream=True)
    write_compressed_ipc_file(resource_dir / 'ipc_zstd_compression.arrow', 'zstd')
    write_compressed_ipc_file(resource_dir / 'ipc_zstd_compression.arrow_stream', 'zstd', stream=True)

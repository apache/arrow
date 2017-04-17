# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# flake8: noqa

from pkg_resources import get_distribution, DistributionNotFound
try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
   # package is not installed
   pass


import pyarrow._config
from pyarrow._config import cpu_count, set_cpu_count

from pyarrow._array import (null, bool_,
                            int8, int16, int32, int64,
                            uint8, uint16, uint32, uint64,
                            timestamp, date32, date64,
                            float16, float32, float64,
                            binary, string, decimal,
                            list_, struct, dictionary, field,
                            DataType, FixedSizeBinaryType,
                            Field, Schema, schema,
                            Array, Tensor,
                            array,
                            from_numpy_dtype,
                            NullArray,
                            NumericArray, IntegerArray, FloatingPointArray,
                            BooleanArray,
                            Int8Array, UInt8Array,
                            Int16Array, UInt16Array,
                            Int32Array, UInt32Array,
                            Int64Array, UInt64Array,
                            ListArray, StringArray,
                            DictionaryArray,
                            ArrayValue, Scalar, NA, NAType,
                            BooleanValue,
                            Int8Value, Int16Value, Int32Value, Int64Value,
                            UInt8Value, UInt16Value, UInt32Value, UInt64Value,
                            FloatValue, DoubleValue, ListValue,
                            BinaryValue, StringValue, FixedSizeBinaryValue)

from pyarrow._io import (HdfsFile, NativeFile, PythonFileInterface,
                         Buffer, BufferReader, InMemoryOutputStream,
                         OSFile, MemoryMappedFile, memory_map,
                         frombuffer, read_tensor, write_tensor,
                         memory_map, create_memory_map,
                         get_record_batch_size, get_tensor_size)

from pyarrow._memory import (MemoryPool, total_allocated_bytes,
                             set_memory_pool, default_memory_pool)
from pyarrow._table import Column, RecordBatch, Table, concat_tables
from pyarrow._error import (ArrowException,
                            ArrowKeyError,
                            ArrowInvalid,
                            ArrowIOError,
                            ArrowMemoryError,
                            ArrowNotImplementedError,
                            ArrowTypeError)


def jemalloc_memory_pool():
    """
    Returns a jemalloc-based memory allocator, which can be passed to
    pyarrow.set_memory_pool
    """
    from pyarrow._jemalloc import default_pool
    return default_pool()


from pyarrow.filesystem import Filesystem, HdfsClient, LocalFilesystem

from pyarrow.ipc import FileReader, FileWriter, StreamReader, StreamWriter


localfs = LocalFilesystem.get_instance()

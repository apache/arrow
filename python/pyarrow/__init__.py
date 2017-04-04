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


import pyarrow.config
from pyarrow.config import cpu_count, set_cpu_count

from pyarrow.array import (Array, Tensor, from_pylist,
                           NumericArray, IntegerArray, FloatingPointArray,
                           BooleanArray,
                           Int8Array, UInt8Array,
                           Int16Array, UInt16Array,
                           Int32Array, UInt32Array,
                           Int64Array, UInt64Array,
                           ListArray, StringArray,
                           DictionaryArray)

from pyarrow.error import (ArrowException,
                           ArrowKeyError,
                           ArrowInvalid,
                           ArrowIOError,
                           ArrowMemoryError,
                           ArrowNotImplementedError,
                           ArrowTypeError)

from pyarrow.filesystem import Filesystem, HdfsClient, LocalFilesystem
from pyarrow.io import (HdfsFile, NativeFile, PythonFileInterface,
                        Buffer, BufferReader, InMemoryOutputStream,
                        MemoryMappedFile, memory_map,
                        frombuffer, read_tensor, write_tensor,
                        memory_map, create_memory_map)

from pyarrow.ipc import FileReader, FileWriter, StreamReader, StreamWriter

from pyarrow.memory import MemoryPool, total_allocated_bytes

from pyarrow.scalar import (ArrayValue, Scalar, NA, NAType,
                            BooleanValue,
                            Int8Value, Int16Value, Int32Value, Int64Value,
                            UInt8Value, UInt16Value, UInt32Value, UInt64Value,
                            FloatValue, DoubleValue, ListValue,
                            BinaryValue, StringValue, FixedSizeBinaryValue)

import pyarrow.schema as _schema

from pyarrow.schema import (null, bool_,
                            int8, int16, int32, int64,
                            uint8, uint16, uint32, uint64,
                            timestamp, date32, date64,
                            float16, float32, float64,
                            binary, string,
                            list_, struct, dictionary, field,
                            DataType, FixedSizeBinaryType,
                            Field, Schema, schema)


from pyarrow.table import Column, RecordBatch, Table, concat_tables


localfs = LocalFilesystem.get_instance()

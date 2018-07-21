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

import os as _os
import sys as _sys

from pkg_resources import get_distribution, DistributionNotFound
try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
   # package is not installed
    try:
        # This code is duplicated from setup.py to avoid a dependency on each
        # other.
        def parse_version(root):
            from setuptools_scm import version_from_scm
            import setuptools_scm.git
            describe = (setuptools_scm.git.DEFAULT_DESCRIBE +
                        " --match 'apache-arrow-[0-9]*'")
            # Strip catchall from the commandline
            describe = describe.replace("--match *.*", "")
            version = setuptools_scm.git.parse(root, describe)
            if not version:
                return version_from_scm(root)
            else:
                return version

        import setuptools_scm
        __version__ = setuptools_scm.get_version('../', parse=parse_version)
    except (ImportError, LookupError):
        __version__ = None


import pyarrow.compat as compat


# Workaround for https://issues.apache.org/jira/browse/ARROW-2657
if _sys.platform in ('linux', 'linux2'):
    compat.import_tensorflow_extension()


from pyarrow.lib import cpu_count, set_cpu_count
from pyarrow.lib import (null, bool_,
                         int8, int16, int32, int64,
                         uint8, uint16, uint32, uint64,
                         time32, time64, timestamp, date32, date64,
                         float16, float32, float64,
                         binary, string, decimal128,
                         list_, struct, union, dictionary, field,
                         type_for_alias,
                         DataType,
                         Field,
                         Schema,
                         schema,
                         Array, Tensor,
                         array, chunked_array, column,
                         from_numpy_dtype,
                         NullArray,
                         NumericArray, IntegerArray, FloatingPointArray,
                         BooleanArray,
                         Int8Array, UInt8Array,
                         Int16Array, UInt16Array,
                         Int32Array, UInt32Array,
                         Int64Array, UInt64Array,
                         ListArray, UnionArray,
                         BinaryArray, StringArray,
                         FixedSizeBinaryArray,
                         DictionaryArray,
                         Date32Array, Date64Array,
                         TimestampArray, Time32Array, Time64Array,
                         Decimal128Array, StructArray,
                         ArrayValue, Scalar, NA, _NULL as NULL,
                         BooleanValue,
                         Int8Value, Int16Value, Int32Value, Int64Value,
                         UInt8Value, UInt16Value, UInt32Value, UInt64Value,
                         HalfFloatValue, FloatValue, DoubleValue, ListValue,
                         BinaryValue, StringValue, FixedSizeBinaryValue,
                         DecimalValue, UnionValue, StructValue, DictionaryValue,
                         Date32Value, Date64Value,
                         Time32Value, Time64Value,
                         TimestampValue)

# Buffers, allocation
from pyarrow.lib import (Buffer, ResizableBuffer, foreign_buffer, py_buffer,
                         compress, decompress, allocate_buffer)

from pyarrow.lib import (MemoryPool, ProxyMemoryPool, total_allocated_bytes,
                         set_memory_pool, default_memory_pool,
                         log_memory_allocations)

from pyarrow.lib import (HdfsFile, NativeFile, PythonFile,
                         FixedSizeBufferWriter,
                         BufferReader, BufferOutputStream,
                         OSFile, MemoryMappedFile, memory_map,
                         create_memory_map, have_libhdfs, have_libhdfs3,
                         MockOutputStream)

from pyarrow.lib import (ChunkedArray, Column, RecordBatch, Table,
                         concat_tables)

from pyarrow.lib import (ArrowException,
                         ArrowKeyError,
                         ArrowInvalid,
                         ArrowIOError,
                         ArrowMemoryError,
                         ArrowNotImplementedError,
                         ArrowTypeError,
                         ArrowSerializationError,
                         PlasmaObjectExists)

# Serialization
from pyarrow.lib import (deserialize_from, deserialize,
                         deserialize_components,
                         serialize, serialize_to, read_serialized,
                         SerializedPyObject, SerializationContext,
                         SerializationCallbackError,
                         DeserializationCallbackError)

from pyarrow.filesystem import FileSystem, LocalFileSystem

from pyarrow.hdfs import HadoopFileSystem
import pyarrow.hdfs as hdfs

from pyarrow.ipc import (Message, MessageReader,
                         RecordBatchFileReader, RecordBatchFileWriter,
                         RecordBatchStreamReader, RecordBatchStreamWriter,
                         read_message, read_record_batch, read_schema,
                         read_tensor, write_tensor,
                         get_record_batch_size, get_tensor_size,
                         open_stream,
                         open_file,
                         serialize_pandas, deserialize_pandas)

localfs = LocalFileSystem.get_instance()

from pyarrow.serialization import (default_serialization_context,
                                   register_default_serialization_handlers,
                                   register_torch_serialization_handlers)

import pyarrow.types as types

# Entry point for starting the plasma store

def _plasma_store_entry_point():
    """Entry point for starting the plasma store.

    This can be used by invoking e.g.
    ``plasma_store -s /tmp/plasma -m 1000000000``
    from the command line and will start the plasma_store executable with the
    given arguments.
    """
    import pyarrow
    plasma_store_executable = _os.path.join(pyarrow.__path__[0],
                                            "plasma_store")
    _os.execv(plasma_store_executable, _sys.argv)

# ----------------------------------------------------------------------
# Deprecations

from pyarrow.util import _deprecate_api  # noqa

frombuffer = _deprecate_api('frombuffer', 'py_buffer', py_buffer, '0.9.0')

# ----------------------------------------------------------------------
# Returning absolute path to the pyarrow include directory (if bundled, e.g. in
# wheels)

def get_include():
    """
    Return absolute path to directory containing Arrow C++ include
    headers. Similar to numpy.get_include
    """
    return _os.path.join(_os.path.dirname(__file__), 'include')


def get_libraries():
    """
    Return list of library names to include in the `libraries` argument for C
    or Cython extensions using pyarrow
    """
    return ['arrow_python']


def get_library_dirs():
    """
    Return lists of directories likely to contain Arrow C++ libraries for
    linking C or Cython extensions using pyarrow
    """
    package_cwd = _os.path.dirname(__file__)

    library_dirs = [package_cwd]

    if _sys.platform == 'win32':
        # TODO(wesm): Is this necessary, or does setuptools within a conda
        # installation add Library\lib to the linker path for MSVC?
        site_packages, _ = _os.path.split(package_cwd)
        python_base_install, _ = _os.path.split(site_packages)
        library_dirs.append(_os.path.join(python_base_install,
                                          'Library', 'lib'))

    return library_dirs

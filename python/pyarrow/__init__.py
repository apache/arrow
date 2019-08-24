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

from __future__ import absolute_import

import os as _os
import sys as _sys

try:
    from ._generated_version import version as __version__
except ImportError:
    # Package is not installed, parse git tag at runtime
    try:
        import setuptools_scm
        # Code duplicated from setup.py to avoid a dependency on each other
        def parse_git(root, **kwargs):
            """
            Parse function for setuptools_scm that ignores tags for non-C++
            subprojects, e.g. apache-arrow-js-XXX tags.
            """
            from setuptools_scm.git import parse
            kwargs['describe_command'] = \
                "git describe --dirty --tags --long --match 'apache-arrow-[0-9].*'"
            return parse(root, **kwargs)
        __version__ = setuptools_scm.get_version('../',
                                                 parse=parse_git)
    except ImportError:
        __version__ = None


import pyarrow.compat as compat

from pyarrow.lib import cpu_count, set_cpu_count
from pyarrow.lib import (null, bool_,
                         int8, int16, int32, int64,
                         uint8, uint16, uint32, uint64,
                         time32, time64, timestamp, date32, date64,
                         float16, float32, float64,
                         binary, string, utf8,
                         large_binary, large_string, large_utf8,
                         decimal128,
                         list_, large_list, struct, union, dictionary, field,
                         type_for_alias,
                         DataType, DictionaryType, StructType,
                         ListType, LargeListType,
                         UnionType, TimestampType, Time32Type, Time64Type,
                         FixedSizeBinaryType, Decimal128Type,
                         BaseExtensionType, ExtensionType,
                         UnknownExtensionType,
                         DictionaryMemo,
                         Field,
                         Schema,
                         schema,
                         Array, Tensor,
                         array, chunked_array, record_batch, table,
                         SparseTensorCSR, SparseTensorCOO,
                         infer_type, from_numpy_dtype,
                         NullArray,
                         NumericArray, IntegerArray, FloatingPointArray,
                         BooleanArray,
                         Int8Array, UInt8Array,
                         Int16Array, UInt16Array,
                         Int32Array, UInt32Array,
                         Int64Array, UInt64Array,
                         ListArray, LargeListArray, UnionArray,
                         BinaryArray, StringArray,
                         LargeBinaryArray, LargeStringArray,
                         FixedSizeBinaryArray,
                         DictionaryArray,
                         Date32Array, Date64Array,
                         TimestampArray, Time32Array, Time64Array,
                         Decimal128Array, StructArray, ExtensionArray,
                         ArrayValue, Scalar, NA, _NULL as NULL,
                         BooleanValue,
                         Int8Value, Int16Value, Int32Value, Int64Value,
                         UInt8Value, UInt16Value, UInt32Value, UInt64Value,
                         HalfFloatValue, FloatValue, DoubleValue,
                         ListValue, LargeListValue,
                         BinaryValue, StringValue,
                         LargeBinaryValue, LargeStringValue,
                         FixedSizeBinaryValue,
                         DecimalValue, UnionValue, StructValue, DictionaryValue,
                         Date32Value, Date64Value,
                         Time32Value, Time64Value,
                         TimestampValue)

# Buffers, allocation
from pyarrow.lib import (Buffer, ResizableBuffer, foreign_buffer, py_buffer,
                         compress, decompress, allocate_buffer)

from pyarrow.lib import (MemoryPool, LoggingMemoryPool, ProxyMemoryPool,
                         total_allocated_bytes, set_memory_pool,
                         default_memory_pool, logging_memory_pool,
                         proxy_memory_pool, log_memory_allocations)

# I/O
from pyarrow.lib import (HdfsFile, NativeFile, PythonFile,
                         CompressedInputStream, CompressedOutputStream,
                         FixedSizeBufferWriter,
                         BufferReader, BufferOutputStream,
                         OSFile, MemoryMappedFile, memory_map,
                         create_memory_map, have_libhdfs, have_libhdfs3,
                         MockOutputStream, input_stream, output_stream)

from pyarrow.lib import (ChunkedArray, RecordBatch, Table,
                         concat_arrays, concat_tables)

# Exceptions
from pyarrow.lib import (ArrowException,
                         ArrowKeyError,
                         ArrowInvalid,
                         ArrowIOError,
                         ArrowMemoryError,
                         ArrowNotImplementedError,
                         ArrowTypeError,
                         ArrowSerializationError)

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
import pyarrow.ipc as ipc


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
                                            "plasma-store-server")
    _os.execv(plasma_store_executable, _sys.argv)

# ----------------------------------------------------------------------
# Deprecations

from pyarrow.util import _deprecate_api  # noqa

# ----------------------------------------------------------------------
# Returning absolute path to the pyarrow include directory (if bundled, e.g. in
# wheels)

def get_include():
    """
    Return absolute path to directory containing Arrow C++ include
    headers. Similar to numpy.get_include
    """
    return _os.path.join(_os.path.dirname(__file__), 'include')


def _get_pkg_config_executable():
    return _os.environ.get('PKG_CONFIG', 'pkg-config')


def _has_pkg_config(pkgname):
    import subprocess
    try:
        return subprocess.call([_get_pkg_config_executable(),
                                '--exists', pkgname]) == 0
    except OSError:
        # TODO: replace with FileNotFoundError once we ditch 2.7
        return False


def _read_pkg_config_variable(pkgname, cli_args):
    import subprocess
    cmd = [_get_pkg_config_executable(), pkgname] + cli_args
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError("pkg-config failed: " + err.decode('utf8'))
    return out.rstrip().decode('utf8')


def get_libraries():
    """
    Return list of library names to include in the `libraries` argument for C
    or Cython extensions using pyarrow
    """
    return ['arrow', 'arrow_python']


def get_library_dirs():
    """
    Return lists of directories likely to contain Arrow C++ libraries for
    linking C or Cython extensions using pyarrow
    """
    package_cwd = _os.path.dirname(__file__)
    library_dirs = [package_cwd]

    def append_library_dir(library_dir):
        if library_dir not in library_dirs:
            library_dirs.append(library_dir)

    # Search library paths via pkg-config. This is necessary if the user
    # installed libarrow and the other shared libraries manually and they
    # are not shipped inside the pyarrow package (see also ARROW-2976).
    pkg_config_executable = _os.environ.get('PKG_CONFIG') or 'pkg-config'
    for pkgname in ["arrow", "arrow_python"]:
        if _has_pkg_config(pkgname):
            library_dir = _read_pkg_config_variable(pkgname,
                                                    ["--libs-only-L"])
            # pkg-config output could be empty if Arrow is installed
            # as a system package.
            if library_dir:
                if not library_dir.startswith("-L"):
                    raise ValueError(
                        "pkg-config --libs-only-L returned unexpected "
                        "value {0!r}".format(library_dir))
                append_library_dir(library_dir[2:])

    if _sys.platform == 'win32':
        # TODO(wesm): Is this necessary, or does setuptools within a conda
        # installation add Library\lib to the linker path for MSVC?
        python_base_install = _os.path.dirname(_sys.executable)
        library_dir = _os.path.join(python_base_install, 'Library', 'lib')

        if _os.path.exists(_os.path.join(library_dir, 'arrow.lib')):
            append_library_dir(library_dir)

    # ARROW-4074: Allow for ARROW_HOME to be set to some other directory
    if _os.environ.get('ARROW_HOME'):
        append_library_dir(_os.path.join(_os.environ['ARROW_HOME'], 'lib'))
    else:
        # Python wheels bundle the Arrow libraries in the pyarrow directory.
        append_library_dir(_os.path.dirname(_os.path.abspath(__file__)))

    return library_dirs

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

from io import BytesIO
import gc
import os
import pytest
import sys

import numpy as np

import pandas as pd

from pyarrow.compat import u, guid
import pyarrow as pa

# ----------------------------------------------------------------------
# Python file-like objects


def test_python_file_write():
    buf = BytesIO()

    f = pa.PythonFile(buf)

    assert f.tell() == 0

    s1 = b'enga\xc3\xb1ado'
    s2 = b'foobar'

    f.write(s1.decode('utf8'))
    assert f.tell() == len(s1)

    f.write(s2)

    expected = s1 + s2

    result = buf.getvalue()
    assert result == expected

    f.close()


def test_python_file_read():
    data = b'some sample data'

    buf = BytesIO(data)
    f = pa.PythonFile(buf, mode='r')

    assert f.size() == len(data)

    assert f.tell() == 0

    assert f.read(4) == b'some'
    assert f.tell() == 4

    f.seek(0)
    assert f.tell() == 0

    f.seek(5)
    assert f.tell() == 5

    v = f.read(50)
    assert v == b'sample data'
    assert len(v) == 11

    f.close()


def test_bytes_reader():
    # Like a BytesIO, but zero-copy underneath for C++ consumers
    data = b'some sample data'
    f = pa.BufferReader(data)
    assert f.tell() == 0

    assert f.size() == len(data)

    assert f.read(4) == b'some'
    assert f.tell() == 4

    f.seek(0)
    assert f.tell() == 0

    f.seek(5)
    assert f.tell() == 5

    assert f.read(50) == b'sample data'

    f.close()


def test_bytes_reader_non_bytes():
    with pytest.raises(ValueError):
        pa.BufferReader(u('some sample data'))


def test_bytes_reader_retains_parent_reference():
    import gc

    # ARROW-421
    def get_buffer():
        data = b'some sample data' * 1000
        reader = pa.BufferReader(data)
        reader.seek(5)
        return reader.read_buffer(6)

    buf = get_buffer()
    gc.collect()
    assert buf.to_pybytes() == b'sample'
    assert buf.parent is not None

# ----------------------------------------------------------------------
# Buffers


def test_buffer_bytes():
    val = b'some data'

    buf = pa.frombuffer(val)
    assert isinstance(buf, pa.Buffer)

    result = buf.to_pybytes()

    assert result == val


def test_buffer_memoryview():
    val = b'some data'

    buf = pa.frombuffer(val)
    assert isinstance(buf, pa.Buffer)

    result = memoryview(buf)

    assert result == val


def test_buffer_bytearray():
    val = bytearray(b'some data')

    buf = pa.frombuffer(val)
    assert isinstance(buf, pa.Buffer)

    result = bytearray(buf)

    assert result == val


def test_buffer_numpy():
    # Make sure creating a numpy array from an arrow buffer works
    byte_array = bytearray(20)
    byte_array[0] = 42
    buf = pa.frombuffer(byte_array)
    array = np.frombuffer(buf, dtype="uint8")
    assert array[0] == byte_array[0]
    assert array.base == buf


def test_buffer_memoryview_is_immutable():
    val = b'some data'

    buf = pa.frombuffer(val)
    assert isinstance(buf, pa.Buffer)

    result = memoryview(buf)

    with pytest.raises(TypeError) as exc:
        result[0] = b'h'
        assert 'cannot modify read-only' in str(exc.value)

    b = bytes(buf)
    with pytest.raises(TypeError) as exc:
        b[0] = b'h'
        assert 'cannot modify read-only' in str(exc.value)


def test_memory_output_stream():
    # 10 bytes
    val = b'dataabcdef'

    f = pa.BufferOutputStream()

    K = 1000
    for i in range(K):
        f.write(val)

    buf = f.get_result()

    assert len(buf) == len(val) * K
    assert buf.to_pybytes() == val * K


def test_inmemory_write_after_closed():
    f = pa.BufferOutputStream()
    f.write(b'ok')
    f.get_result()

    with pytest.raises(IOError):
        f.write(b'not ok')


def test_buffer_protocol_ref_counting():
    def make_buffer(bytes_obj):
        return bytearray(pa.frombuffer(bytes_obj))

    buf = make_buffer(b'foo')
    gc.collect()
    assert buf == b'foo'

    # ARROW-1053
    val = b'foo'
    refcount_before = sys.getrefcount(val)
    for i in range(10):
        make_buffer(val)
    gc.collect()
    assert refcount_before == sys.getrefcount(val)


def test_nativefile_write_memoryview():
    f = pa.BufferOutputStream()
    data = b'ok'

    arr = np.frombuffer(data, dtype='S1')

    f.write(arr)
    f.write(bytearray(data))

    buf = f.get_result()

    assert buf.to_pybytes() == data * 2


# ----------------------------------------------------------------------
# Mock output stream


def test_mock_output_stream():
    # Make sure that the MockOutputStream and the BufferOutputStream record the
    # same size

    # 10 bytes
    val = b'dataabcdef'

    f1 = pa.MockOutputStream()
    f2 = pa.BufferOutputStream()

    K = 1000
    for i in range(K):
        f1.write(val)
        f2.write(val)

    assert f1.size() == len(f2.get_result())

    # Do the same test with a pandas DataFrame
    val = pd.DataFrame({'a': [1, 2, 3]})
    record_batch = pa.RecordBatch.from_pandas(val)

    f1 = pa.MockOutputStream()
    f2 = pa.BufferOutputStream()

    stream_writer1 = pa.RecordBatchStreamWriter(f1, record_batch.schema)
    stream_writer2 = pa.RecordBatchStreamWriter(f2, record_batch.schema)

    stream_writer1.write_batch(record_batch)
    stream_writer2.write_batch(record_batch)
    stream_writer1.close()
    stream_writer2.close()

    assert f1.size() == len(f2.get_result())


# ----------------------------------------------------------------------
# OS files and memory maps


@pytest.fixture
def sample_disk_data(request, tmpdir):
    SIZE = 4096
    arr = np.random.randint(0, 256, size=SIZE).astype('u1')
    data = arr.tobytes()[:SIZE]

    path = os.path.join(str(tmpdir), guid())

    with open(path, 'wb') as f:
        f.write(data)

    def teardown():
        _try_delete(path)
    request.addfinalizer(teardown)
    return path, data


def _check_native_file_reader(FACTORY, sample_data):
    path, data = sample_data

    f = FACTORY(path, mode='r')

    assert f.read(10) == data[:10]
    assert f.read(0) == b''
    assert f.tell() == 10

    assert f.read() == data[10:]

    assert f.size() == len(data)

    f.seek(0)
    assert f.tell() == 0

    # Seeking past end of file not supported in memory maps
    f.seek(len(data) + 1)
    assert f.tell() == len(data) + 1
    assert f.read(5) == b''

    # Test whence argument of seek, ARROW-1287
    assert f.seek(3) == 3
    assert f.seek(3, os.SEEK_CUR) == 6
    assert f.tell() == 6

    ex_length = len(data) - 2
    assert f.seek(-2, os.SEEK_END) == ex_length
    assert f.tell() == ex_length


def test_memory_map_reader(sample_disk_data):
    _check_native_file_reader(pa.memory_map, sample_disk_data)


def test_memory_map_retain_buffer_reference(sample_disk_data):
    path, data = sample_disk_data

    cases = []
    with pa.memory_map(path, 'rb') as f:
        cases.append((f.read_buffer(100), data[:100]))
        cases.append((f.read_buffer(100), data[100:200]))
        cases.append((f.read_buffer(100), data[200:300]))

    # Call gc.collect() for good measure
    gc.collect()

    for buf, expected in cases:
        assert buf.to_pybytes() == expected


def test_os_file_reader(sample_disk_data):
    _check_native_file_reader(pa.OSFile, sample_disk_data)


def _try_delete(path):
    try:
        os.remove(path)
    except os.error:
        pass


def test_memory_map_writer(tmpdir):
    SIZE = 4096
    arr = np.random.randint(0, 256, size=SIZE).astype('u1')
    data = arr.tobytes()[:SIZE]

    path = os.path.join(str(tmpdir), guid())
    with open(path, 'wb') as f:
        f.write(data)

    f = pa.memory_map(path, mode='r+b')

    f.seek(10)
    f.write('peekaboo')
    assert f.tell() == 18

    f.seek(10)
    assert f.read(8) == b'peekaboo'

    f2 = pa.memory_map(path, mode='r+b')

    f2.seek(10)
    f2.write(b'booapeak')
    f2.seek(10)

    f.seek(10)
    assert f.read(8) == b'booapeak'

    # Does not truncate file
    f3 = pa.memory_map(path, mode='w')
    f3.write('foo')

    with pa.memory_map(path) as f4:
        assert f4.size() == SIZE

    with pytest.raises(IOError):
        f3.read(5)

    f.seek(0)
    assert f.read(3) == b'foo'


def test_os_file_writer(tmpdir):
    SIZE = 4096
    arr = np.random.randint(0, 256, size=SIZE).astype('u1')
    data = arr.tobytes()[:SIZE]

    path = os.path.join(str(tmpdir), guid())
    with open(path, 'wb') as f:
        f.write(data)

    # Truncates file
    f2 = pa.OSFile(path, mode='w')
    f2.write('foo')

    with pa.OSFile(path) as f3:
        assert f3.size() == 3

    with pytest.raises(IOError):
        f2.read(5)


def test_native_file_modes(tmpdir):
    path = os.path.join(str(tmpdir), guid())
    with open(path, 'wb') as f:
        f.write(b'foooo')

    with pa.OSFile(path, mode='r') as f:
        assert f.mode == 'rb'

    with pa.OSFile(path, mode='rb') as f:
        assert f.mode == 'rb'

    with pa.OSFile(path, mode='w') as f:
        assert f.mode == 'wb'

    with pa.OSFile(path, mode='wb') as f:
        assert f.mode == 'wb'

    with open(path, 'wb') as f:
        f.write(b'foooo')

    with pa.memory_map(path, 'r') as f:
        assert f.mode == 'rb'

    with pa.memory_map(path, 'r+') as f:
        assert f.mode == 'rb+'

    with pa.memory_map(path, 'r+b') as f:
        assert f.mode == 'rb+'

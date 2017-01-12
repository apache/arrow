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
import os
import pytest

import numpy as np

from pyarrow.compat import u, guid
import pyarrow.io as io
import pyarrow as pa

# ----------------------------------------------------------------------
# Python file-like objects


def test_python_file_write():
    buf = BytesIO()

    f = io.PythonFileInterface(buf)

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
    f = io.PythonFileInterface(buf, mode='r')

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
    f = io.BytesReader(data)

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
        io.BytesReader(u('some sample data'))


def test_bytes_reader_retains_parent_reference():
    import gc

    # ARROW-421
    def get_buffer():
        data = b'some sample data' * 1000
        reader = io.BytesReader(data)
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

    buf = io.buffer_from_bytes(val)
    assert isinstance(buf, io.Buffer)

    result = buf.to_pybytes()

    assert result == val


def test_memory_output_stream():
    # 10 bytes
    val = b'dataabcdef'

    f = io.InMemoryOutputStream()

    K = 1000
    for i in range(K):
        f.write(val)

    buf = f.get_result()

    assert len(buf) == len(val) * K
    assert buf.to_pybytes() == val * K


def test_inmemory_write_after_closed():
    f = io.InMemoryOutputStream()
    f.write(b'ok')
    f.get_result()

    with pytest.raises(IOError):
        f.write(b'not ok')


# ----------------------------------------------------------------------
# OS files and memory maps

@pytest.fixture(scope='session')
def sample_disk_data(request):

    SIZE = 4096
    arr = np.random.randint(0, 256, size=SIZE).astype('u1')
    data = arr.tobytes()[:SIZE]

    path = guid()
    with open(path, 'wb') as f:
        f.write(data)

    def teardown():
        _try_delete(path)
    request.addfinalizer(teardown)
    return path, data


def _check_native_file_reader(KLASS, sample_data):
    path, data = sample_data

    f = KLASS(path, mode='r')

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


def test_memory_map_reader(sample_disk_data):
    _check_native_file_reader(io.MemoryMappedFile, sample_disk_data)


def test_os_file_reader(sample_disk_data):
    _check_native_file_reader(io.OSFile, sample_disk_data)


def _try_delete(path):
    try:
        os.remove(path)
    except os.error:
        pass


def test_memory_map_writer():
    SIZE = 4096
    arr = np.random.randint(0, 256, size=SIZE).astype('u1')
    data = arr.tobytes()[:SIZE]

    path = guid()
    try:
        with open(path, 'wb') as f:
            f.write(data)

        f = io.MemoryMappedFile(path, mode='r+w')

        f.seek(10)
        f.write('peekaboo')
        assert f.tell() == 18

        f.seek(10)
        assert f.read(8) == b'peekaboo'

        f2 = io.MemoryMappedFile(path, mode='r+w')

        f2.seek(10)
        f2.write(b'booapeak')
        f2.seek(10)

        f.seek(10)
        assert f.read(8) == b'booapeak'

        # Does not truncate file
        f3 = io.MemoryMappedFile(path, mode='w')
        f3.write('foo')

        with io.MemoryMappedFile(path) as f4:
            assert f4.size() == SIZE

        with pytest.raises(IOError):
            f3.read(5)

        f.seek(0)
        assert f.read(3) == b'foo'
    finally:
        _try_delete(path)


def test_os_file_writer():
    SIZE = 4096
    arr = np.random.randint(0, 256, size=SIZE).astype('u1')
    data = arr.tobytes()[:SIZE]

    path = guid()
    try:
        with open(path, 'wb') as f:
            f.write(data)

        # Truncates file
        f2 = io.OSFile(path, mode='w')
        f2.write('foo')

        with io.OSFile(path) as f3:
            assert f3.size() == 3

        with pytest.raises(IOError):
            f2.read(5)
    finally:
        _try_delete(path)

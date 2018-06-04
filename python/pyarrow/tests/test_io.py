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

from io import BytesIO, TextIOWrapper
import gc
import os
import pickle
import pytest
import sys
import tempfile
import weakref

import numpy as np

import pandas as pd

from pyarrow.compat import u, guid
import pyarrow as pa


def check_large_seeks(file_factory):
    if sys.platform in ('win32', 'darwin'):
        pytest.skip("need sparse file support")
    try:
        filename = tempfile.mktemp(prefix='test_io')
        with open(filename, 'wb') as f:
            f.truncate(2 ** 32 + 10)
            f.seek(2 ** 32 + 5)
            f.write(b'mark\n')
        with file_factory(filename) as f:
            assert f.seek(2 ** 32 + 5) == 2 ** 32 + 5
            assert f.tell() == 2 ** 32 + 5
            assert f.read(5) == b'mark\n'
            assert f.tell() == 2 ** 32 + 10
    finally:
        os.unlink(filename)


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

    assert f.size() == len(data)

    f.close()


def test_python_file_large_seeks():
    def factory(filename):
        return pa.PythonFile(open(filename, 'rb'))

    check_large_seeks(factory)


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
    with pytest.raises(TypeError):
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


def test_python_file_implicit_mode(tmpdir):
    path = os.path.join(str(tmpdir), 'foo.txt')
    with open(path, 'wb') as f:
        pf = pa.PythonFile(f)
        assert pf.writable()
        assert not pf.readable()
        assert not pf.seekable()  # PyOutputStream isn't seekable
        f.write(b'foobar\n')

    with open(path, 'rb') as f:
        pf = pa.PythonFile(f)
        assert pf.readable()
        assert not pf.writable()
        assert pf.seekable()
        assert pf.read() == b'foobar\n'

    bio = BytesIO()
    pf = pa.PythonFile(bio)
    assert pf.writable()
    assert not pf.readable()
    assert not pf.seekable()
    pf.write(b'foobar\n')
    assert bio.getvalue() == b'foobar\n'


def test_python_file_closing():
    bio = BytesIO()
    pf = pa.PythonFile(bio)
    wr = weakref.ref(pf)
    del pf
    assert wr() is None  # object was destroyed
    assert not bio.closed
    pf = pa.PythonFile(bio)
    pf.close()
    assert bio.closed


# ----------------------------------------------------------------------
# Buffers


def test_buffer_bytes():
    val = b'some data'

    buf = pa.py_buffer(val)
    assert isinstance(buf, pa.Buffer)
    assert not buf.is_mutable

    result = buf.to_pybytes()

    assert result == val

    # Check that buffers survive a pickle roundtrip
    result_buf = pickle.loads(pickle.dumps(buf))
    result = result_buf.to_pybytes()
    assert result == val


def test_buffer_memoryview():
    val = b'some data'

    buf = pa.py_buffer(val)
    assert isinstance(buf, pa.Buffer)
    assert not buf.is_mutable

    result = memoryview(buf)

    assert result == val


def test_buffer_bytearray():
    val = bytearray(b'some data')

    buf = pa.py_buffer(val)
    assert isinstance(buf, pa.Buffer)
    assert buf.is_mutable

    result = bytearray(buf)

    assert result == val


def test_buffer_invalid():
    with pytest.raises(TypeError,
                       match="(bytes-like object|buffer interface)"):
        pa.py_buffer(None)


def test_buffer_to_numpy():
    # Make sure creating a numpy array from an arrow buffer works
    byte_array = bytearray(20)
    byte_array[0] = 42
    buf = pa.py_buffer(byte_array)
    array = np.frombuffer(buf, dtype="uint8")
    assert array[0] == byte_array[0]
    byte_array[0] += 1
    assert array[0] == byte_array[0]
    assert array.base == buf


def test_buffer_from_numpy():
    # C-contiguous
    arr = np.arange(12, dtype=np.int8).reshape((3, 4))
    buf = pa.py_buffer(arr)
    assert buf.to_pybytes() == arr.tobytes()
    # F-contiguous; note strides informations is lost
    buf = pa.py_buffer(arr.T)
    assert buf.to_pybytes() == arr.tobytes()
    # Non-contiguous
    with pytest.raises(ValueError, match="not contiguous"):
        buf = pa.py_buffer(arr.T[::2])


def test_buffer_equals():
    # Buffer.equals() returns true iff the buffers have the same contents
    def eq(a, b):
        assert a.equals(b)
        assert a == b
        assert not (a != b)

    def ne(a, b):
        assert not a.equals(b)
        assert not (a == b)
        assert a != b

    b1 = b'some data!'
    b2 = bytearray(b1)
    b3 = bytearray(b1)
    b3[0] = 42
    buf1 = pa.py_buffer(b1)
    buf2 = pa.py_buffer(b2)
    buf3 = pa.py_buffer(b2)
    buf4 = pa.py_buffer(b3)
    buf5 = pa.py_buffer(np.frombuffer(b2, dtype=np.int16))
    eq(buf1, buf1)
    eq(buf1, buf2)
    eq(buf2, buf3)
    ne(buf2, buf4)
    # Data type is indifferent
    eq(buf2, buf5)


def test_buffer_hashing():
    # Buffers are unhashable
    with pytest.raises(TypeError, match="unhashable"):
        hash(pa.py_buffer(b'123'))


def test_foreign_buffer():
    obj = np.array([1, 2], dtype=np.int32)
    addr = obj.__array_interface__["data"][0]
    size = obj.nbytes
    buf = pa.foreign_buffer(addr, size, obj)
    wr = weakref.ref(obj)
    del obj
    assert np.frombuffer(buf, dtype=np.int32).tolist() == [1, 2]
    assert wr() is not None
    del buf
    assert wr() is None


def test_allocate_buffer():
    buf = pa.allocate_buffer(100)
    assert buf.size == 100
    assert buf.is_mutable

    bit = b'abcde'
    writer = pa.FixedSizeBufferWriter(buf)
    writer.write(bit)

    assert buf.to_pybytes()[:5] == bit


def test_allocate_buffer_resizable():
    buf = pa.allocate_buffer(100, resizable=True)
    assert isinstance(buf, pa.ResizableBuffer)

    buf.resize(200)
    assert buf.size == 200


def test_compress_decompress():
    INPUT_SIZE = 10000
    test_data = (np.random.randint(0, 255, size=INPUT_SIZE)
                 .astype(np.uint8)
                 .tostring())
    test_buf = pa.py_buffer(test_data)

    codecs = ['lz4', 'snappy', 'gzip', 'zstd', 'brotli']
    for codec in codecs:
        compressed_buf = pa.compress(test_buf, codec=codec)
        compressed_bytes = pa.compress(test_data, codec=codec, asbytes=True)

        assert isinstance(compressed_bytes, bytes)

        decompressed_buf = pa.decompress(compressed_buf, INPUT_SIZE,
                                         codec=codec)
        decompressed_bytes = pa.decompress(compressed_bytes, INPUT_SIZE,
                                           codec=codec, asbytes=True)

        assert isinstance(decompressed_bytes, bytes)

        assert decompressed_buf.equals(test_buf)
        assert decompressed_bytes == test_data

        with pytest.raises(ValueError):
            pa.decompress(compressed_bytes, codec=codec)


def test_buffer_memoryview_is_immutable():
    val = b'some data'

    buf = pa.py_buffer(val)
    assert not buf.is_mutable
    assert isinstance(buf, pa.Buffer)

    result = memoryview(buf)
    assert result.readonly

    with pytest.raises(TypeError) as exc:
        result[0] = b'h'
        assert 'cannot modify read-only' in str(exc.value)

    b = bytes(buf)
    with pytest.raises(TypeError) as exc:
        b[0] = b'h'
        assert 'cannot modify read-only' in str(exc.value)


def test_uninitialized_buffer():
    # ARROW-2039: calling Buffer() directly creates an uninitialized object
    # ARROW-2638: prevent calling extension class constructors directly
    with pytest.raises(TypeError):
        pa.Buffer()


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

    with pytest.raises(ValueError):
        f.write(b'not ok')


def test_buffer_protocol_ref_counting():
    def make_buffer(bytes_obj):
        return bytearray(pa.py_buffer(bytes_obj))

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


def test_os_file_large_seeks():
    check_large_seeks(pa.OSFile)


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


def test_memory_zero_length(tmpdir):
    path = os.path.join(str(tmpdir), guid())
    f = open(path, 'wb')
    f.close()
    with pa.memory_map(path, mode='r+b') as memory_map:
        assert memory_map.size() == 0


def test_memory_map_large_seeks():
    check_large_seeks(pa.memory_map)


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
        assert f.readable()
        assert not f.writable()
        assert f.seekable()

    with pa.OSFile(path, mode='rb') as f:
        assert f.mode == 'rb'
        assert f.readable()
        assert not f.writable()
        assert f.seekable()

    with pa.OSFile(path, mode='w') as f:
        assert f.mode == 'wb'
        assert not f.readable()
        assert f.writable()
        assert not f.seekable()

    with pa.OSFile(path, mode='wb') as f:
        assert f.mode == 'wb'
        assert not f.readable()
        assert f.writable()
        assert not f.seekable()

    with open(path, 'wb') as f:
        f.write(b'foooo')

    with pa.memory_map(path, 'r') as f:
        assert f.mode == 'rb'
        assert f.readable()
        assert not f.writable()
        assert f.seekable()

    with pa.memory_map(path, 'r+') as f:
        assert f.mode == 'rb+'
        assert f.readable()
        assert f.writable()
        assert f.seekable()

    with pa.memory_map(path, 'r+b') as f:
        assert f.mode == 'rb+'
        assert f.readable()
        assert f.writable()
        assert f.seekable()


def test_native_file_raises_ValueError_after_close(tmpdir):
    path = os.path.join(str(tmpdir), guid())
    with open(path, 'wb') as f:
        f.write(b'foooo')

    with pa.OSFile(path, mode='rb') as os_file:
        assert not os_file.closed
    assert os_file.closed

    with pa.memory_map(path, mode='rb') as mmap_file:
        assert not mmap_file.closed
    assert mmap_file.closed

    files = [os_file,
             mmap_file]

    methods = [('tell', ()),
               ('seek', (0,)),
               ('size', ()),
               ('flush', ()),
               ('readable', ()),
               ('writable', ()),
               ('seekable', ())]

    for f in files:
        for method, args in methods:
            with pytest.raises(ValueError):
                getattr(f, method)(*args)


def test_native_file_TextIOWrapper(tmpdir):
    data = (u'foooo\n'
            u'barrr\n'
            u'bazzz\n')

    path = os.path.join(str(tmpdir), guid())
    with open(path, 'wb') as f:
        f.write(data.encode('utf-8'))

    with TextIOWrapper(pa.OSFile(path, mode='rb')) as fil:
        assert fil.readable()
        res = fil.read()
        assert res == data
    assert fil.closed

    with TextIOWrapper(pa.OSFile(path, mode='rb')) as fil:
        # Iteration works
        lines = list(fil)
        assert ''.join(lines) == data

    # Writing
    path2 = os.path.join(str(tmpdir), guid())
    with TextIOWrapper(pa.OSFile(path2, mode='wb')) as fil:
        assert fil.writable()
        fil.write(data)

    with TextIOWrapper(pa.OSFile(path2, mode='rb')) as fil:
        res = fil.read()
        assert res == data

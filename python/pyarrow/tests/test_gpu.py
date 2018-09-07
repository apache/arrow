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

"""
UNTESTED:
CudaDeviceManager.create_new_context
CudaContext.open_ipc_buffer
CudaIpcMemHandle.from_buffer
CudaIpcMemHandle.serialize
CudaBuffer.from_buffer
CudaBuffer.export_for_ipc
CudaBuffer.context
cuda_serialize_record_batch
cuda_read_message
cuda_read_record_batch
"""


import pytest
import pyarrow as pa
import numpy as np

gpu_support = pytest.mark.skipif(not pa.has_gpu_support, reason='Arrow not built with GPU support')

def setup_module(module):
    if pa.has_gpu_support:
        module.manager = pa.CudaDeviceManager()
        module.context = module.manager.get_context()
        
def teardown_module(module):
    if pa.has_gpu_support:
        module.context.close()

@gpu_support
def test_manager_num_devices():
    assert manager.num_devices > 0

    # expected to fail, but fails only occasionally:
    #manager.get_context(manager.num_devices+1) 

@gpu_support
def test_manage_allocate_free_host():
    size = 1024
    buf = manager.allocate_host(size)
    arr = np.frombuffer(buf, dtype=np.uint8)
    arr[size//4:3*size//4] = 1
    arr_cp = arr.copy()
    arr2 = np.frombuffer(buf, dtype=np.uint8)
    assert arr2.tolist() == arr_cp.tolist()
    assert buf.size == size
    manager.free_host(buf)
    assert buf.size == 0
    arr3 = np.frombuffer(buf, dtype=np.uint8)
    assert arr3.tolist() == []

    buf = pa.allocate_cuda_host_buffer(size)
    arr = np.frombuffer(buf, dtype=np.uint8)
    arr[size//4:3*size//4] = 1
    arr_cp = arr.copy()
    arr2 = np.frombuffer(buf, dtype=np.uint8)
    assert arr2.tolist() == arr_cp.tolist()
    assert buf.size == size
    manager.free_host(buf)
    assert buf.size == 0
    arr3 = np.frombuffer(buf, dtype=np.uint8)
    assert arr3.tolist() == []
    
@gpu_support
def test_manage_allocate_autofree_host():
    size = 1024
    buf = manager.allocate_host(size)
    del buf

@gpu_support
def test_context_allocate_del():
    bytes_allocated = context.bytes_allocated
    cudabuf = context.allocate(128)
    assert context.bytes_allocated == bytes_allocated + 128
    del cudabuf
    assert context.bytes_allocated == bytes_allocated

    
def make_random_buffer(size, target='host'):
    if target == 'host':
        assert size >= 0
        buf = pa.allocate_buffer(size, resizable=True)
        assert buf.size == size
        arr = np.frombuffer(buf, dtype=np.uint8)
        assert arr.size == size
        arr[:] = np.random.randint(low=0, high=255, size=size, dtype=np.uint8)
        assert arr.sum()>0
        arr_ = np.frombuffer(buf, dtype=np.uint8)
        assert (arr == arr_).all()
        return arr, buf
    if target == 'device':
        arr, buf = make_random_buffer(size, target='host')
        dbuf = context.allocate(size)
        assert dbuf.size == size
        dbuf.copy_from_host(buf, position=0, nbytes=size)
        return arr, dbuf
    raise ValueError('invalid target value')


@gpu_support
def test_context_device_buffer():
    # Creating device buffer from host buffer;
    size = 8
    arr, buf = make_random_buffer(size)
    cudabuf = context.device_buffer(buf)
    assert cudabuf.size == size
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    assert arr.tolist() == arr2.tolist()

    #arr3 = np.frombuffer(cudabuf, dtype=np.uint8) # crashes!!
    #assert arr.tolist() == arr3.tolist()

    # Creating device buffer from array:
    cudabuf = context.device_buffer(arr)
    assert cudabuf.size == size
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    assert arr.tolist() == arr2.tolist()

    # Creating device buffer from bytes:
    cudabuf = context.device_buffer(arr.tobytes())
    assert cudabuf.size == size
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    assert arr.tolist() == arr2.tolist()
    
    # Creating device buffer from another device buffer:
    cudabuf2 = context.device_buffer(cudabuf)
    assert cudabuf2.size == size
    arr2 = np.frombuffer(cudabuf2.copy_to_host(), dtype=np.uint8)
    assert arr.tolist() == arr2.tolist()

    # Creating a device buffer from a slice of host buffer
    soffset = size//4
    ssize = 2*size//4
    cudabuf = context.device_buffer(buf, offset=soffset, size=ssize)
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    assert arr[soffset:soffset + ssize].tolist() == arr2.tolist()

    cudabuf = context.device_buffer(buf.slice(offset=soffset, length=ssize))
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    assert arr[soffset:soffset + ssize].tolist() == arr2.tolist()

    # Creating a device buffer from a slice of an array
    cudabuf = context.device_buffer(arr, offset=soffset, size=ssize)
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    assert arr[soffset:soffset + ssize].tolist() == arr2.tolist()

    cudabuf = context.device_buffer(arr[soffset:soffset+ssize])
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    assert arr[soffset:soffset + ssize].tolist() == arr2.tolist()

    # Creating a device buffer from a slice of bytes
    cudabuf = context.device_buffer(arr.tobytes(), offset=soffset, size=ssize)
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    assert arr[soffset:soffset + ssize].tolist() == arr2.tolist()

    # Creating a device buffer from size
    cudabuf = context.device_buffer(size=size)
    assert cudabuf.size == size

    # Creating device buffer from a slice of another device buffer:
    cudabuf = context.device_buffer(arr)
    cudabuf2 = context.device_buffer(cudabuf, offset=soffset, size=ssize)
    assert cudabuf2.size == ssize
    arr2 = np.frombuffer(cudabuf2.copy_to_host(), dtype=np.uint8)
    assert arr[soffset:soffset+ssize].tolist() == arr2.tolist()

    # Creating device buffer from CudaHostBuffer

    buf = manager.allocate_host(size)
    arr_ = np.frombuffer(buf, dtype=np.uint8)
    arr_[:] = arr
    cudabuf = context.device_buffer(buf)
    assert cudabuf.size == size
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    assert arr.tolist() == arr2.tolist()

    # Creating device buffer from CudaHostBuffer slice

    cudabuf = context.device_buffer(buf, offset=soffset, size=ssize)
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    assert arr[soffset:soffset+ssize].tolist() == arr2.tolist()

    cudabuf = context.device_buffer(buf.slice(offset=soffset, length=ssize))
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    assert arr[soffset:soffset+ssize].tolist() == arr2.tolist()

@gpu_support
def test_CudaBuffer():
    size = 8
    arr, buf = make_random_buffer(size)
    assert arr.tobytes() == buf.to_pybytes()
    cbuf = context.device_buffer(buf)
    assert cbuf.size == size
    assert arr.tobytes() == cbuf.to_pybytes()

    for i in range(size):
        assert cbuf[i] == arr[i]

    for s in [
            slice(None),
            slice(size//4, size//2),
    ]:
        assert cbuf[s].to_pybytes() == arr[s].tobytes()

    sbuf = cbuf.slice(size//4, size//2)
    assert sbuf.parent == cbuf
        
    with pytest.raises(TypeError) as e_info:
        pa.CudaBuffer()
        assert str(e_info).startswith("Do not call CudaBuffer's constructor directly")

@gpu_support
def test_CudaHostBuffer():
    size = 8
    arr, buf = make_random_buffer(size)
    assert arr.tobytes() == buf.to_pybytes()
    hbuf = manager.allocate_host(size)
    np.frombuffer(hbuf, dtype=np.uint8)[:] = arr
    assert hbuf.size == size
    assert arr.tobytes() == hbuf.to_pybytes()
    for i in range(size):
        assert hbuf[i] == arr[i]
    for s in [
            slice(None),
            slice(size//4, size//2),
    ]:
        assert hbuf[s].to_pybytes() == arr[s].tobytes()

    sbuf = hbuf.slice(size//4, size//2)
    assert sbuf.parent == hbuf
        
    del hbuf

    with pytest.raises(TypeError) as e_info:
        pa.CudaHostBuffer()
        assert str(e_info).startswith("Do not call CudaHostBuffer's constructor directly")

    
@gpu_support
def test_copy_from_to_host():
    size = 1024

    # Create a buffer in host containing range(size)
    buf = pa.allocate_buffer(size, resizable=True) # in host
    assert isinstance(buf, pa.Buffer)
    assert not isinstance(buf, pa.CudaBuffer)
    arr = np.frombuffer(buf, dtype=np.uint8)
    assert arr.size == size
    arr[:] = range(size)
    arr_ = np.frombuffer(buf, dtype=np.uint8)
    assert (arr==arr_).all()
    
    device_buffer = context.allocate(size)
    assert isinstance(device_buffer, pa.CudaBuffer)
    assert isinstance(device_buffer, pa.Buffer)
    assert device_buffer.size == size
    
    device_buffer.copy_from_host(buf, position=0, nbytes=size)
    
    buf2 = device_buffer.copy_to_host(position=0, nbytes=size)
    arr2 = np.frombuffer(buf2, dtype=np.uint8)
    assert (arr==arr2).all()
    
@gpu_support
def test_copy_to_host():
    size = 1024
    arr, dbuf = make_random_buffer(size, target='device')

    buf = dbuf.copy_to_host()
    assert (arr == np.frombuffer(buf, dtype=np.uint8)).all()

    buf = dbuf.copy_to_host(position=size//4)
    assert (arr[size//4:] == np.frombuffer(buf, dtype=np.uint8)).all()

    buf = dbuf.copy_to_host(position=size//4, nbytes=size//8)
    assert (arr[size//4:size//4+size//8] == np.frombuffer(buf, dtype=np.uint8)).all()

    buf = dbuf.copy_to_host(position=size//4, nbytes=0)
    assert buf.size == 0

    for (position, nbytes) in [
        (size+2, -1), (-2, -1), (size+1,0), (-3,0),
    ]:
        with pytest.raises(ValueError) as e_info:
            dbuf.copy_to_host(position=position, nbytes=nbytes)
        assert str(e_info.value).startswith('position argument is out-of-range')

    for (position, nbytes) in [
        (0, size+1), (size//2, size//2+1), (size, 1)
    ]:
        with pytest.raises(ValueError) as e_info:
            dbuf.copy_to_host(position=position, nbytes=nbytes)
        assert str(e_info.value).startswith('requested more to copy than available from device buffer')

    buf = pa.allocate_buffer(size//4)
    dbuf.copy_to_host(buf=buf)
    assert (arr[:size//4] == np.frombuffer(buf, dtype=np.uint8)).all()

    dbuf.copy_to_host(buf=buf, position=12)
    assert (arr[12:12+size//4] == np.frombuffer(buf, dtype=np.uint8)).all()
    
    dbuf.copy_to_host(buf=buf, nbytes=12)
    assert (arr[:12] == np.frombuffer(buf, dtype=np.uint8)[:12]).all()

    dbuf.copy_to_host(buf=buf, nbytes=12, position=6)
    assert (arr[6:6+12] == np.frombuffer(buf, dtype=np.uint8)[:12]).all()

    for (position, nbytes) in [
            (0, size + 10), (10, size-5),
            (0, size//2), (size//4, size//4+1)
    ]:
        with pytest.raises(ValueError) as e_info:
            dbuf.copy_to_host(buf=buf, position=position, nbytes=nbytes)
            print('dbuf.size={}, buf.size={}, position={}, nbytes={}'
                  .format(dbuf.size, buf.size, position, nbytes))
        assert str(e_info.value).startswith('requested copy does not fit into host buffer')

@gpu_support
def test_copy_from_host():
    size = 1024
    arr, buf = make_random_buffer(size=size, target='host')
    lst = arr.tolist()
    dbuf = context.allocate(size)

    def put(*args, **kwargs):
        nbytes = dbuf.copy_from_host(buf, *args, **kwargs)
        rbuf = dbuf.copy_to_host()
        return np.frombuffer(rbuf, dtype=np.uint8).tolist()
    assert put() == lst
    assert put(position=size//4) == lst[:size//4]+lst[:-size//4]
    assert put() == lst
    assert put(position=1, nbytes=size//2) == lst[:1] + lst[:size//2] + lst[-(size-size//2-1):]
    
    for (position, nbytes) in [
            (size+2, -1), (-2, -1), (size+1,0), (-3,0),
    ]:
        with pytest.raises(ValueError) as e_info:
            put(position=position, nbytes=nbytes)
            print('dbuf.size={}, buf.size={}, position={}, nbytes={}'
                  .format(dbuf.size, buf.size, position, nbytes))
        assert str(e_info.value).startswith('position argument is out-of-range')

    for (position, nbytes) in [
        (0, size+1),
    ]:
        with pytest.raises(ValueError) as e_info:
            put(position=position, nbytes=nbytes)
            print('dbuf.size={}, buf.size={}, position={}, nbytes={}'
                  .format(dbuf.size, buf.size, position, nbytes))
        assert str(e_info.value).startswith('requested more to copy than available from host buffer')

    for (position, nbytes) in [
        (size//2, size//2+1)
    ]:
        with pytest.raises(ValueError) as e_info:
            put(position=position, nbytes=nbytes)
            print('dbuf.size={}, buf.size={}, position={}, nbytes={}'
                  .format(dbuf.size, buf.size, position, nbytes))
        assert str(e_info.value).startswith('requested more to copy than available in device buffer')

        
@gpu_support
def test_CudaBufferWriter():
    
    def allocate(size):
        cbuf = context.allocate(size)
        writer = pa.CudaBufferWriter(cbuf)
        return cbuf, writer
    
    def test_writes(total_bytes, chunksize, buffer_size = 0):
        cbuf, writer = allocate(total_size)
        arr, buf = make_random_buffer(size=total_size, target='host')

        if buffer_size > 0:
            writer.set_buffer_size(buffer_size)

        position = writer.tell()
        assert position == 0
        writer.write(buf.slice(length=chunksize))
        assert writer.tell() == chunksize
        writer.seek(0) 
        position = writer.tell()
        assert position == 0

        while position < total_bytes:
            bytes_to_write = min(chunksize, total_bytes - position)
            writer.write(buf.slice(offset=position, length=bytes_to_write))
            position += bytes_to_write

        writer.flush()
        assert cbuf.size == total_bytes
        buf2 = cbuf.copy_to_host()
        assert buf2.size == total_bytes
        arr2 = np.frombuffer(buf2, dtype=np.uint8)
        assert arr2.size == total_bytes
        assert (arr2 == arr).all()
        
    total_size, chunk_size = 1<<16, 1000
    test_writes(total_size, chunk_size)
    test_writes(total_size, chunk_size, total_size // (1 << 4))

    cbuf, writer = allocate(100)
    writer.write(np.arange(100, dtype=np.uint8))
    writer.writeat(50, np.arange(25, dtype=np.uint8))
    writer.write(np.arange(25, dtype=np.uint8))
    writer.flush()

    arr = np.frombuffer(cbuf.copy_to_host(), np.uint8)
    assert (arr[:50]==np.arange(50, dtype=np.uint8)).all()
    assert (arr[50:75]==np.arange(25, dtype=np.uint8)).all()
    assert (arr[75:]==np.arange(25, dtype=np.uint8)).all()


@gpu_support
def test_CudaBufferWriter_edge_cases():
    # edge cases:
    size = 1000
    cbuf = context.allocate(size)
    writer = pa.CudaBufferWriter(cbuf)
    arr, buf = make_random_buffer(size=size, target='host')

    assert writer.buffer_size == 0
    writer.set_buffer_size(100)
    assert writer.buffer_size == 100

    writer.write(buf.slice(length=0))
    assert writer.tell() == 0

    writer.write(buf.slice(length=10))
    writer.set_buffer_size(200)
    assert writer.buffer_size == 200
    assert writer.num_bytes_buffered == 0

    writer.write(buf.slice(offset=10, length=300))
    assert writer.num_bytes_buffered == 0

    writer.write(buf.slice(offset=310, length=200))
    assert writer.num_bytes_buffered == 0

    writer.write(buf.slice(offset=510, length=390))
    writer.write(buf.slice(offset=900, length=100))

    writer.flush()
    
    buf2 = cbuf.copy_to_host()
    assert buf2.size == size
    arr2 = np.frombuffer(buf2, dtype=np.uint8)
    assert arr2.size == size
    assert (arr2 == arr).all()

    
    
@gpu_support
def test_CudaBufferReader():

    size = 1000
    arr, cbuf = make_random_buffer(size=size, target='device')

    reader = pa.CudaBufferReader(cbuf)
    reader.seek(950)
    assert reader.tell() == 950

    data = reader.read(100)
    assert len(data) == 50
    assert reader.tell() == 1000

    reader.seek(925)
    arr2 = np.zeros(100, dtype=np.uint8)
    n = reader.readinto(arr2)
    assert n == 75
    assert reader.tell() == 1000
    assert (arr2[:75] == arr[925:]).all()

    reader.seek(0)
    assert reader.tell() == 0
    buf2 = reader.read_buffer()
    arr2 = np.frombuffer(buf2.copy_to_host(), dtype=np.uint8)
    assert (arr2 == arr).all()

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

from pyarrow.compat import tobytes
from pyarrow.lib cimport *
from pyarrow.includes.libarrow_cuda cimport *
from pyarrow.lib import py_buffer, allocate_buffer, as_buffer
cimport cpython as cp


cdef class Context:
    """ CUDA driver context.
    """

    def __cinit__(self, int device_number=0, uintptr_t handle=0):
        """Construct the shared CUDA driver context for a particular device.

        Parameters
        ----------
        device_number : int
          Specify the gpu device for which the CUDA driver context is
          requested.
        handle : int
          Specify handle for a shared context that has been created by
          another library.
        """
        cdef CCudaDeviceManager* manager
        check_status(CCudaDeviceManager.GetInstance(&manager))
        cdef int n = manager.num_devices()
        if device_number >= n or device_number < 0:
            self.context.reset()
            raise ValueError('device_number argument must be '
                             'non-negative less than %s' % (n))
        if handle == 0:
            check_status(manager.GetContext(device_number, &self.context))
        else:
            check_status(manager.GetSharedContext(device_number,
                                                  <void*>handle,
                                                  &self.context))
        self.device_number = device_number

    @staticmethod
    def from_numba(context=None):
        """Create Context instance from a numba CUDA context.

        Parameters
        ----------
        context : {numba.cuda.cudadrv.driver.Context, None}
          Specify numba CUDA context instance. When None, use the
          current numba context.

        Returns
        -------
        shared_context : pyarrow.cuda.Context
          Context instance.
        """
        if context is None:
            import numba.cuda
            context = numba.cuda.current_context()
        return Context(device_number=context.device.id,
                       handle=context.handle.value)

    def to_numba(self):
        """Convert Context to numba CUDA context.

        Returns
        -------
        context : numba.cuda.cudadrv.driver.Context
          Numba CUDA context instance.
        """
        import ctypes
        import numba.cuda
        device = numba.cuda.gpus[self.device_number]
        handle = ctypes.c_void_p(self.handle)
        context = numba.cuda.cudadrv.driver.Context(device, handle)

        class DummyPendingDeallocs(object):
            # Context is managed by pyarrow
            def add_item(self, *args, **kwargs):
                pass

        context.deallocations = DummyPendingDeallocs()
        return context

    @staticmethod
    def get_num_devices():
        """ Return the number of GPU devices.
        """
        cdef CCudaDeviceManager* manager
        check_status(CCudaDeviceManager.GetInstance(&manager))
        return manager.num_devices()

    @property
    def device_number(self):
        """ Return context device number.
        """
        return self.device_number

    @property
    def handle(self):
        """ Return pointer to context handle.
        """
        return <uintptr_t>self.context.get().handle()

    cdef void init(self, const shared_ptr[CCudaContext]& ctx):
        self.context = ctx

    def synchronize(self):
        """Blocks until the device has completed all preceding requested
        tasks.
        """
        check_status(self.context.get().Synchronize())

    @property
    def bytes_allocated(self):
        """ Return the number of allocated bytes.
        """
        return self.context.get().bytes_allocated()

    def new_buffer(self, nbytes):
        """Return new device buffer.

        Parameters
        ----------
        nbytes : int
          Specify the number of bytes to be allocated.

        Returns
        -------
        buf : CudaBuffer
          Allocated buffer.
        """
        cdef shared_ptr[CCudaBuffer] cudabuf
        check_status(self.context.get().Allocate(nbytes, &cudabuf))
        return pyarrow_wrap_cudabuffer(cudabuf)

    def foreign_buffer(self, address, size):
        """Create device buffer from device address and size as a view.

        The caller is responsible for allocating and freeing the
        memory as well as ensuring that the memory belongs to the
        CUDA context that this Context instance holds.

        Parameters
        ----------
        address : int
          Specify the starting address of the buffer.
        size : int
          Specify the size of device buffer in bytes.

        Returns
        -------
        cbuf : CudaBuffer
          Device buffer as a view of device memory.
        """
        cdef:
            intptr_t c_addr = address
            int64_t c_size = size
            shared_ptr[CCudaBuffer] cudabuf
        check_status(self.context.get().View(<uint8_t*>c_addr,
                                             c_size,
                                             &cudabuf))
        return pyarrow_wrap_cudabuffer(cudabuf)

    def open_ipc_buffer(self, ipc_handle):
        """ Open existing CUDA IPC memory handle

        Parameters
        ----------
        ipc_handle : IpcMemHandle
          Specify opaque pointer to CUipcMemHandle (driver API).

        Returns
        -------
        buf : CudaBuffer
          referencing device buffer
        """
        handle = pyarrow_unwrap_cudaipcmemhandle(ipc_handle)
        cdef shared_ptr[CCudaBuffer] cudabuf
        check_status(self.context.get().OpenIpcBuffer(handle.get()[0],
                                                      &cudabuf))
        return pyarrow_wrap_cudabuffer(cudabuf)

    def buffer_from_data(self, object data, int64_t offset=0, int64_t size=-1):
        """Create device buffer and initalize with data.

        Parameters
        ----------
        data : {HostBuffer, Buffer, array-like}
          Specify data to be copied to device buffer.
        offset : int
          Specify the offset of input buffer for device data
          buffering. Default: 0.
        size : int
          Specify the size of device buffer in bytes. Default: all
          (starting from input offset)

        Returns
        -------
        cbuf : CudaBuffer
          Device buffer with copied data.
        """
        if pyarrow_is_cudabuffer(data):
            raise NotImplementedError('copying data from device to device')

        buf = as_buffer(data)

        bsize = buf.size
        if offset < 0 or offset >= bsize:
            raise ValueError('offset argument is out-of-range')
        if size < 0:
            size = bsize - offset
        elif offset + size > bsize:
            raise ValueError(
                'requested larger slice than available in device buffer')

        if offset != 0 or size != bsize:
            buf = buf.slice(offset, size)

        result = self.new_buffer(size)
        result.copy_from_host(buf, position=0, nbytes=size)
        return result


cdef class IpcMemHandle:
    """A container for a CUDA IPC handle.
    """
    cdef void init(self, shared_ptr[CCudaIpcMemHandle]& h):
        self.handle = h

    @staticmethod
    def from_buffer(Buffer opaque_handle):
        """Create IpcMemHandle from opaque buffer (e.g. from another
        process)

        Parameters
        ----------
        opaque_handle :
          a CUipcMemHandle as a const void*

        Results
        -------
        ipc_handle : IpcMemHandle
        """
        cdef shared_ptr[CCudaIpcMemHandle] handle
        buf_ = pyarrow_unwrap_buffer(opaque_handle)
        check_status(CCudaIpcMemHandle.FromBuffer(buf_.get().data(), &handle))
        return pyarrow_wrap_cudaipcmemhandle(handle)

    def serialize(self, pool=None):
        """Write IpcMemHandle to a Buffer

        Parameters
        ----------
        pool : {MemoryPool, None}
          Specify a pool to allocate memory from

        Returns
        -------
        buf : Buffer
          The serialized buffer.
        """
        cdef CMemoryPool* pool_ = maybe_unbox_memory_pool(pool)
        cdef shared_ptr[CBuffer] buf
        cdef CCudaIpcMemHandle* h = self.handle.get()
        check_status(h.Serialize(pool_, &buf))
        return pyarrow_wrap_buffer(buf)


cdef class CudaBuffer(Buffer):
    """An Arrow buffer with data located in a GPU device.

    To create a CudaBuffer instance, use

      <Context instance>.device_buffer(data=<object>, offset=<offset>,
                                       size=<nbytes>)

    The memory allocated in CudaBuffer instance is freed when the
    instance is deleted.

    """

    def __init__(self):
        raise TypeError("Do not call CudaBuffer's constructor directly, use "
                        "`<pyarrow.Context instance>.device_buffer`"
                        " method instead.")

    cdef void init_cuda(self, const shared_ptr[CCudaBuffer]& buffer):
        self.cuda_buffer = buffer
        self.init(<shared_ptr[CBuffer]> buffer)

    @staticmethod
    def from_buffer(buf):
        """ Convert back generic buffer into CudaBuffer

        Parameters
        ----------
        buf : Buffer
          Specify buffer containing CudaBuffer

        Returns
        -------
        dbuf : CudaBuffer
          Resulting device buffer.
        """
        buf_ = pyarrow_unwrap_buffer(buf)
        cdef shared_ptr[CCudaBuffer] cbuf
        check_status(CCudaBuffer.FromBuffer(buf_, &cbuf))
        return pyarrow_wrap_cudabuffer(cbuf)

    @staticmethod
    def from_numba(mem):
        """Create a CudaBuffer view from numba MemoryPointer instance.

        Parameters
        ----------
        mem :  numba.cuda.cudadrv.driver.MemoryPointer

        Returns
        -------
        cbuf : CudaBuffer
          Device buffer as a view of numba MemoryPointer.
        """
        ctx = Context.from_numba(mem.context)
        return ctx.foreign_buffer(mem.device_pointer.value, mem.size)

    def to_numba(self):
        """Return numba memory pointer of CudaBuffer instance.
        """
        import ctypes
        from numba.cuda.cudadrv.driver import MemoryPointer
        return MemoryPointer(self.context.to_numba(),
                             pointer=ctypes.c_void_p(self.address),
                             size=self.size)

    cdef getitem(self, int64_t i):
        return self.copy_to_host(position=i, nbytes=1)[0]

    def copy_to_host(self, int64_t position=0, int64_t nbytes=-1,
                     Buffer buf=None,
                     MemoryPool memory_pool=None, c_bool resizable=False):
        """Copy memory from GPU device to CPU host

        Caller is responsible for ensuring that all tasks affecting
        the memory are finished. Use

          `<CudaBuffer instance>.context.synchronize()`

        when needed.

        Parameters
        ----------
        position : int
          Specify the starting position of the source data in GPU
          device buffer. Default: 0.
        nbytes : int
          Specify the number of bytes to copy. Default: -1 (all from
          the position until host buffer is full).
        buf : Buffer
          Specify a pre-allocated output buffer in host. Default: None
          (allocate new output buffer).
        memory_pool : MemoryPool
        resizable : bool
          Specify extra arguments to allocate_buffer. Used only when
          buf is None.

        Returns
        -------
        buf : Buffer
          Output buffer in host.

        """
        if position < 0 or position > self.size:
            raise ValueError('position argument is out-of-range')
        cdef int64_t nbytes_
        if buf is None:
            if nbytes < 0:
                # copy all starting from position to new host buffer
                nbytes_ = self.size - position
            else:
                if nbytes > self.size - position:
                    raise ValueError(
                        'requested more to copy than available from '
                        'device buffer')
                # copy nbytes starting from position to new host buffeer
                nbytes_ = nbytes
            buf = allocate_buffer(nbytes_, memory_pool=memory_pool,
                                  resizable=resizable)
        else:
            if nbytes < 0:
                # copy all from position until given host buffer is full
                nbytes_ = min(self.size - position, buf.size)
            else:
                if nbytes > buf.size:
                    raise ValueError(
                        'requested copy does not fit into host buffer')
                # copy nbytes from position to given host buffer
                nbytes_ = nbytes
        cdef shared_ptr[CBuffer] buf_ = pyarrow_unwrap_buffer(buf)
        cdef int64_t position_ = position
        with nogil:
            check_status(self.cuda_buffer.get()
                         .CopyToHost(position_, nbytes_,
                                     buf_.get().mutable_data()))
        return buf

    def copy_from_host(self, data, int64_t position=0, int64_t nbytes=-1):
        """Copy data from host to device.

        The device buffer must be pre-allocated.

        Parameters
        ----------
        data : {Buffer, array-like}
          Specify data in host. It can be array-like that is valid
          argument to py_buffer
        position : int
          Specify the starting position of the copy in devive buffer.
          Default: 0.
        nbytes : int
          Specify the number of bytes to copy. Default: -1 (all from
          source until device buffer, starting from position, is full)

        Returns
        -------
        nbytes : int
          Number of bytes copied.
        """
        if position < 0 or position > self.size:
            raise ValueError('position argument is out-of-range')
        cdef int64_t nbytes_
        buf = as_buffer(data)

        if nbytes < 0:
            # copy from host buffer to device buffer starting from
            # position until device buffer is full
            nbytes_ = min(self.size - position, buf.size)
        else:
            if nbytes > buf.size:
                raise ValueError(
                    'requested more to copy than available from host buffer')
            if nbytes > self.size - position:
                raise ValueError(
                    'requested more to copy than available in device buffer')
            # copy nbytes from host buffer to device buffer starting
            # from position
            nbytes_ = nbytes

        cdef shared_ptr[CBuffer] buf_ = pyarrow_unwrap_buffer(buf)
        cdef int64_t position_ = position
        with nogil:
            check_status(self.cuda_buffer.get().
                         CopyFromHost(position_, buf_.get().data(), nbytes_))
        return nbytes_

    def export_for_ipc(self):
        """
        Expose this device buffer as IPC memory which can be used in other
        processes.

        After calling this function, this device memory will not be
        freed when the CudaBuffer is destructed.

        Results
        -------
        ipc_handle : IpcMemHandle
          The exported IPC handle

        """
        cdef shared_ptr[CCudaIpcMemHandle] handle
        check_status(self.cuda_buffer.get().ExportForIpc(&handle))
        return pyarrow_wrap_cudaipcmemhandle(handle)

    @property
    def context(self):
        """Returns the CUDA driver context of this buffer.
        """
        return pyarrow_wrap_cudacontext(self.cuda_buffer.get().context())

    def slice(self, offset=0, length=None):
        """Return slice of device buffer

        Parameters
        ----------
        offset : int, default 0
          Specify offset from the start of device buffer to slice
        length : int, default None
          Specify the length of slice (default is until end of device
          buffer starting from offset)

        Returns
        -------
        sliced : CudaBuffer
          Zero-copy slice of device buffer.
        """
        if offset < 0 or offset >= self.size:
            raise ValueError('offset argument is out-of-range')
        cdef int64_t offset_ = offset
        cdef int64_t size
        if length is None:
            size = self.size - offset_
        elif offset + length <= self.size:
            size = length
        else:
            raise ValueError(
                'requested larger slice than available in device buffer')
        parent = pyarrow_unwrap_cudabuffer(self)
        return pyarrow_wrap_cudabuffer(make_shared[CCudaBuffer](parent,
                                                                offset_, size))

    def to_pybytes(self):
        """Return device buffer content as Python bytes.
        """
        return self.copy_to_host().to_pybytes()

    def __getbuffer__(self, cp.Py_buffer* buffer, int flags):
        # Device buffer contains data pointers on the device. Hence,
        # cannot support buffer protocol PEP-3118 for CudaBuffer.
        raise BufferError('buffer protocol for device buffer not supported')

    def __getreadbuffer__(self, Py_ssize_t idx, void** p):
        # Python 2.x specific method
        raise NotImplementedError('CudaBuffer.__getreadbuffer__')

    def __getwritebuffer__(self, Py_ssize_t idx, void** p):
        # Python 2.x specific method
        raise NotImplementedError('CudaBuffer.__getwritebuffer__')


cdef class HostBuffer(Buffer):
    """Device-accessible CPU memory created using cudaHostAlloc.

    To create a HostBuffer instance, use

      cuda.new_host_buffer(<nbytes>)
    """

    def __init__(self):
        raise TypeError("Do not call HostBuffer's constructor directly,"
                        " use `cuda.new_host_buffer` function instead.")

    cdef void init_host(self, const shared_ptr[CCudaHostBuffer]& buffer):
        self.host_buffer = buffer
        self.init(<shared_ptr[CBuffer]> buffer)

    @property
    def size(self):
        return self.host_buffer.get().size()


cdef class BufferReader(NativeFile):
    """File interface for zero-copy read from CUDA buffers.

    Note: Read methods return pointers to device memory. This means
    you must be careful using this interface with any Arrow code which
    may expect to be able to do anything other than pointer arithmetic
    on the returned buffers.
    """
    def __cinit__(self, CudaBuffer obj):
        self.buffer = obj
        self.reader = new CCudaBufferReader(self.buffer.buffer)
        self.rd_file.reset(self.reader)
        self.is_readable = True
        self.closed = False

    def read_buffer(self, nbytes=None):
        """Return a slice view of the underlying device buffer.

        The slice will start at the current reader position and will
        have specified size in bytes.

        Parameters
        ----------
        nbytes : int, default None
          Specify the number of bytes to read. Default: None (read all
          remaining bytes).

        Returns
        -------
        cbuf : CudaBuffer
          New device buffer.

        """
        cdef:
            int64_t c_nbytes
            int64_t bytes_read = 0
            shared_ptr[CCudaBuffer] output

        if nbytes is None:
            c_nbytes = self.size() - self.tell()
        else:
            c_nbytes = nbytes

        with nogil:
            check_status(self.reader.Read(c_nbytes,
                                          <shared_ptr[CBuffer]*> &output))

        return pyarrow_wrap_cudabuffer(output)


cdef class BufferWriter(NativeFile):
    """File interface for writing to CUDA buffers.

    By default writes are unbuffered. Use set_buffer_size to enable
    buffering.
    """
    def __cinit__(self, CudaBuffer buffer):
        self.buffer = buffer
        self.writer = new CCudaBufferWriter(self.buffer.cuda_buffer)
        self.wr_file.reset(self.writer)
        self.is_writable = True
        self.closed = False

    def writeat(self, int64_t position, object data):
        """Write data to buffer starting from position.

        Parameters
        ----------
        position : int
          Specify device buffer position where the data will be
          written.
        data : array-like
          Specify data, the data instance must implement buffer
          protocol.
        """
        cdef Buffer arrow_buffer = as_buffer(data)
        cdef const uint8_t* buf = arrow_buffer.buffer.get().data()
        cdef int64_t bufsize = len(arrow_buffer)
        with nogil:
            check_status(self.writer.WriteAt(position, buf, bufsize))

    def flush(self):
        """ Flush the buffer stream """
        with nogil:
            check_status(self.writer.Flush())

    def seek(self, int64_t position, int whence=0):
        # TODO: remove this method after NativeFile.seek supports
        # writable files.
        cdef int64_t offset

        with nogil:
            if whence == 0:
                offset = position
            elif whence == 1:
                check_status(self.writer.Tell(&offset))
                offset = offset + position
            else:
                with gil:
                    raise ValueError("Invalid value of whence: {0}"
                                     .format(whence))
            check_status(self.writer.Seek(offset))
        return self.tell()

    @property
    def buffer_size(self):
        """Returns size of host (CPU) buffer, 0 for unbuffered
        """
        return self.writer.buffer_size()

    @buffer_size.setter
    def buffer_size(self, int64_t buffer_size):
        """Set CPU buffer size to limit calls to cudaMemcpy

        Parameters
        ----------
        buffer_size : int
          Specify the size of CPU buffer to allocate in bytes.
        """
        with nogil:
            check_status(self.writer.SetBufferSize(buffer_size))

    @property
    def num_bytes_buffered(self):
        """Returns number of bytes buffered on host
        """
        return self.writer.num_bytes_buffered()

# Functions


def new_host_buffer(const int64_t size, int device=0):
    """Return buffer with CUDA-accessible memory on CPU host

    Parameters
    ----------
    size : int
      Specify the number of bytes to be allocated.
    device : int
      Specify GPU device number.

    Returns
    -------
    dbuf : HostBuffer
      Allocated host buffer
    """
    cdef shared_ptr[CCudaHostBuffer] buffer
    check_status(AllocateCudaHostBuffer(device, size, &buffer))
    return pyarrow_wrap_cudahostbuffer(buffer)


def serialize_record_batch(object batch, object ctx):
    """ Write record batch message to GPU device memory

    Parameters
    ----------
    batch : RecordBatch
      Specify record batch to write
    ctx : Context
      Specify context to allocate device memory from

    Returns
    -------
    dbuf : CudaBuffer
      device buffer which contains the record batch message
    """
    cdef shared_ptr[CCudaBuffer] buffer
    cdef CRecordBatch* batch_ = pyarrow_unwrap_batch(batch).get()
    cdef CCudaContext* ctx_ = pyarrow_unwrap_cudacontext(ctx).get()
    with nogil:
        check_status(CudaSerializeRecordBatch(batch_[0], ctx_, &buffer))
    return pyarrow_wrap_cudabuffer(buffer)


def read_message(object source, pool=None):
    """ Read Arrow IPC message located on GPU device

    Parameters
    ----------
    source : {CudaBuffer, cuda.BufferReader}
      Specify device buffer or reader of device buffer.
    pool : {MemoryPool, None}
      Specify pool to allocate CPU memory for the metadata

    Returns
    -------
    message : Message
      the deserialized message, body still on device
    """
    cdef:
        Message result = Message.__new__(Message)
    cdef CMemoryPool* pool_ = maybe_unbox_memory_pool(pool)
    if not isinstance(source, BufferReader):
        reader = BufferReader(source)
    check_status(CudaReadMessage(reader.reader, pool_, &result.message))
    return result


def read_record_batch(object buffer, object schema, pool=None):
    """Construct RecordBatch referencing IPC message located on CUDA device.

    While the metadata is copied to host memory for deserialization,
    the record batch data remains on the device.

    Parameters
    ----------
    buffer :
      Specify device buffer containing the complete IPC message
    schema : Schema
      Specify schema for the record batch
    pool : {MemoryPool, None}
      Specify pool to use for allocating space for the metadata

    Returns
    -------
    batch : RecordBatch
      reconstructed record batch, with device pointers

    """
    cdef shared_ptr[CSchema] schema_ = pyarrow_unwrap_schema(schema)
    cdef shared_ptr[CCudaBuffer] buffer_ = pyarrow_unwrap_cudabuffer(buffer)
    cdef CMemoryPool* pool_ = maybe_unbox_memory_pool(pool)
    cdef shared_ptr[CRecordBatch] batch
    check_status(CudaReadRecordBatch(schema_, buffer_, pool_, &batch))
    return pyarrow_wrap_batch(batch)


# Public API


cdef public api bint pyarrow_is_buffer(object buffer):
    return isinstance(buffer, Buffer)

# cudabuffer

cdef public api bint pyarrow_is_cudabuffer(object buffer):
    return isinstance(buffer, CudaBuffer)


cdef public api object \
        pyarrow_wrap_cudabuffer(const shared_ptr[CCudaBuffer]& buf):
    cdef CudaBuffer result = CudaBuffer.__new__(CudaBuffer)
    result.init_cuda(buf)
    return result


cdef public api shared_ptr[CCudaBuffer] pyarrow_unwrap_cudabuffer(object obj):
    if pyarrow_is_cudabuffer(obj):
        return (<CudaBuffer>obj).cuda_buffer
    raise TypeError('expected CudaBuffer instance, got %s'
                    % (type(obj).__name__))

# cudahostbuffer

cdef public api bint pyarrow_is_cudahostbuffer(object buffer):
    return isinstance(buffer, HostBuffer)


cdef public api object \
        pyarrow_wrap_cudahostbuffer(const shared_ptr[CCudaHostBuffer]& buf):
    cdef HostBuffer result = HostBuffer.__new__(HostBuffer)
    result.init_host(buf)
    return result


cdef public api shared_ptr[CCudaHostBuffer] \
        pyarrow_unwrap_cudahostbuffer(object obj):
    if pyarrow_is_cudahostbuffer(obj):
        return (<HostBuffer>obj).host_buffer
    raise TypeError('expected HostBuffer instance, got %s'
                    % (type(obj).__name__))

# cudacontext

cdef public api bint pyarrow_is_cudacontext(object ctx):
    return isinstance(ctx, Context)


cdef public api object \
        pyarrow_wrap_cudacontext(const shared_ptr[CCudaContext]& ctx):
    cdef Context result = Context.__new__(Context)
    result.init(ctx)
    return result


cdef public api shared_ptr[CCudaContext] \
        pyarrow_unwrap_cudacontext(object obj):
    if pyarrow_is_cudacontext(obj):
        return (<Context>obj).context
    raise TypeError('expected Context instance, got %s'
                    % (type(obj).__name__))

# cudaipcmemhandle

cdef public api bint pyarrow_is_cudaipcmemhandle(object handle):
    return isinstance(handle, IpcMemHandle)


cdef public api object \
        pyarrow_wrap_cudaipcmemhandle(shared_ptr[CCudaIpcMemHandle]& h):
    cdef IpcMemHandle result = IpcMemHandle.__new__(IpcMemHandle)
    result.init(h)
    return result


cdef public api shared_ptr[CCudaIpcMemHandle] \
        pyarrow_unwrap_cudaipcmemhandle(object obj):
    if pyarrow_is_cudaipcmemhandle(obj):
        return (<IpcMemHandle>obj).handle
    raise TypeError('expected IpcMemHandle instance, got %s'
                    % (type(obj).__name__))

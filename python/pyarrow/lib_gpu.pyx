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

import six
from pyarrow.compat import tobytes
from pyarrow.lib cimport *
from pyarrow.includes.libarrow_gpu cimport *
from pyarrow.lib import py_buffer, allocate_buffer
cimport cpython as cp

cdef class CudaDeviceManager:
    """ CUDA device manager.
    """

    def __cinit__(self):
        check_status(CCudaDeviceManager.GetInstance(& self.manager))

    @property
    def num_devices(self):
        """ Return the number of GPU devices.
        """
        return self.manager.num_devices()

    def get_context(self, gpu_number=0):
        """Get the shared CUDA driver context for a particular device.

        Parameters
        ----------
        gpu_number : int
          Specify the gpu device for which the CUDA driver context is
          requested.

        Returns
        -------
        ctx : CudaContext
          CUDA driver context.

        """
        cdef shared_ptr[CCudaContext] ctx
        check_status(self.manager.GetContext(gpu_number, & ctx))
        return pyarrow_wrap_cudacontext(ctx)

    def create_new_context(self, gpu_number):
        """ Create a new context for a given device number.

        Use get_context for general code.
        """
        cdef shared_ptr[CCudaContext] ctx
        check_status(self.manager.CreateNewContext(gpu_number, & ctx))
        return pyarrow_wrap_cudacontext(ctx)

    def allocate_host(self, nbytes):
        """ Allocate host memory.

        Parameters
        ----------
        nbytes : int
          Specify the number of bytes to be allocated.

        Returns
        -------
        hbuf : CudaHostBuffer
          Buffer of allocated host memory.
        """
        cdef shared_ptr[CCudaHostBuffer] buf
        check_status(self.manager.AllocateHost(nbytes, & buf))
        return pyarrow_wrap_cudahostbuffer(buf)


cdef class CudaContext:
    """ CUDA driver context
    """

    cdef void init(self, const shared_ptr[CCudaContext] & ctx):
        self.context = ctx

    def close(self):
        """ Close context.
        """
        check_status(self.context.get().Close())

    @property
    def bytes_allocated(self):
        """ Return the number of allocated bytes.
        """
        return self.context.get().bytes_allocated()

    def allocate(self, nbytes):
        """ Allocate CUDA memory on GPU device for this context.

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
        check_status(self.context.get().Allocate(nbytes, & cudabuf))
        return pyarrow_wrap_cudabuffer(cudabuf)

    def open_ipc_buffer(self, ipc_handle):
        """ Open existing CUDA IPC memory handle

        Parameters
        ----------
        ipc_handle : CudaIpcMemHandle
          Specify opaque pointer to CUipcMemHandle (driver API).

        Returns
        -------
        buf : CudaBuffer
          referencing device buffer
        """
        handle = pyarrow_unwrap_cudaipcmemhandle(ipc_handle)
        cdef shared_ptr[CCudaBuffer] cudabuf
        check_status(self.context.get().OpenIpcBuffer(handle.get()[0],
                                                      & cudabuf))
        return pyarrow_wrap_cudabuffer(cudabuf)

    def device_buffer(self, object obj=None,
                      int64_t offset=0, int64_t size=-1):
        """Create device buffer from object data.

        Parameters
        ----------
        obj : {CudaBuffer, Buffer, array-like, None}
          Specify data for device buffer.
        offset : int
          Specify the offset of input buffer for device data
          buffering. Default: 0.
        size : int
          Specify the size of device buffer in bytes. Default: all
          (starting from input offset)

        Returns
        -------
        cbuf : CudaBuffer
          Device buffer containing object data.
        """
        if obj is None:
            if size < 0:
                raise ValueError('buffer size must be positive int')
            return self.allocate(size)
        if pyarrow_is_cudabuffer(obj):
            # TODO: check if obj.context == self or at least are of
            # the same device.
            if size < 0:
                length = None
            else:
                length = size
            return obj.slice(offset, length)

        cdef shared_ptr[CBuffer] buf

        if not pyarrow_is_buffer(obj):  # assume obj is array-like
            check_status(PyBuffer.FromPyObject(obj, & buf))
            obj = pyarrow_wrap_buffer(buf)

        bsize = obj.size
        if offset < 0 or offset >= bsize:
            raise ValueError('offset argument is out-of-range')
        if size < 0:
            size = bsize - offset
        elif offset + size > bsize:
            raise ValueError(
                'requested larger slice than available in device buffer')

        if offset != 0 or size != bsize:
            obj = obj.slice(offset, size)

        if pyarrow_is_cudahostbuffer(obj):
            buf = < shared_ptr[CBuffer] > pyarrow_unwrap_cudahostbuffer(obj)
        elif pyarrow_is_buffer(obj):
            buf = pyarrow_unwrap_buffer(obj)
        else:
            raise TypeError('Expected Buffer or array-like but got %s'
                            % (type(obj).__name__))

        result = self.allocate(size)
        result.copy_from_host(pyarrow_wrap_buffer(buf),
                              position=0,
                              nbytes=size)
        return result


cdef class CudaIpcMemHandle:
    """A container for a CUDA IPC handle.
    """
    cdef void init(self, shared_ptr[CCudaIpcMemHandle] h):
        self.handle = h

    @staticmethod
    def from_buffer(Buffer opaque_handle):
        """Create CudaIpcMemHandle from opaque buffer (e.g. from another
        process)

        Parameters
        ----------
        opaque_handle :
          a CUipcMemHandle as a const void*

        Results
        -------
        ipc_handle : CudaIpcMemHandle
        """
        cdef shared_ptr[CCudaIpcMemHandle] handle
        buf_ = pyarrow_unwrap_buffer(opaque_handle)
        check_status(CCudaIpcMemHandle.FromBuffer(buf_.get().data(), & handle))
        return pyarrow_wrap_cudaipcmemhandle(handle)

    def serialize(self, pool=None):
        """Write CudaIpcMemHandle to a Buffer

        Parameters
        ----------
        pool : {MemoryPool, None}
          Specify a pool to allocate memory from

        Returns
        -------
        buf : Buffer
          The serialized buffer.
        """
        pool_ = maybe_unbox_memory_pool(pool)
        cdef shared_ptr[CBuffer] buf
        cdef CCudaIpcMemHandle * h = self.handle.get()
        check_status(h.Serialize(pool_, & buf))
        return pyarrow_wrap_buffer(buf)


cdef class CudaBuffer(Buffer):
    """An Arrow buffer with data located in a GPU device.

    To create a CudaBuffer instance, use

      <CudaContext instance>.device_buffer(obj=<object>, offset=<offset>,
                                           size=<nbytes>)

    The memory allocated in CudaBuffer instance is freed when the
    instance is deleted.

    """

    def __init__(self):
        raise TypeError("Do not call CudaBuffer's constructor directly, use "
                        "`<pyarrow.CudaContext instance>.device_buffer`"
                        " method instead.")

    cdef void init_cuda(self, const shared_ptr[CCudaBuffer] & buffer):
        self.cuda_buffer = buffer
        self.init(< shared_ptr[CBuffer] > buffer)

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
        check_status(CCudaBuffer.FromBuffer(buf_, & cbuf))
        return pyarrow_wrap_cudabuffer(cbuf)

    cdef getitem(self, int64_t i):
        return self.copy_to_host(position=i, nbytes=1)[0]

    def copy_to_host(self, int64_t position=0, int64_t nbytes=-1,
                     Buffer buf=None,
                     MemoryPool memory_pool=None, c_bool resizable=False):
        """Copy memory from GPU device to CPU host

        Parameters
        ----------
        position : int
          Specify the starting position of the copy in GPU
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
        check_status(self.cuda_buffer.get()
                     .CopyToHost(position, nbytes_, buf_.get().mutable_data()))
        return buf

    def copy_from_host(self, source, int64_t position=0, int64_t nbytes=-1):
        """Copy memory from CPU host to GPU device.

        The device buffer must be pre-allocated.

        Parameters
        ----------
        source : {Buffer, buffer-like}
          Specify source of data in host. It can be buffer-like that
          is valid argument to py_buffer
        position : int
          Specify the starting position of the copy in GPU devive
          buffer.  Default: 0.
        nbytes : int
          Specify the number of bytes to copy. Default: -1 (all from
          source until device buffer starting from position is full)

        Returns
        -------
        nbytes : int
          Number of bytes copied.
        """
        if position < 0 or position > self.size:
            raise ValueError('position argument is out-of-range')
        buf = source if isinstance(source, Buffer) else py_buffer(source)

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
        check_status(self.cuda_buffer.get().
                     CopyFromHost(position, buf_.get().data(), nbytes_))
        return nbytes_

    def export_for_ipc(self):
        """
        Expose this device buffer as IPC memory which can be used in other
        processes.

        After calling this function, this device memory will not be
        freed when the CudaBuffer is destructed.

        Results
        -------
        ipc_handle : CudaIpcMemHandle
          The exported IPC handle

        """
        cdef shared_ptr[CCudaIpcMemHandle] handle
        check_status(self.cuda_buffer.get().ExportForIpc(& handle))
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
        return pyarrow_wrap_cudabuffer(
            shared_ptr[CCudaBuffer](make_shared[CCudaBuffer](parent,
                                                             offset_, size)))

    def to_pybytes(self):
        return self.copy_to_host().to_pybytes()

    def __getbuffer__(self, cp.Py_buffer * buffer, int flags):
        # copy_to_host() is needed otherwise, the buffered device
        # buffer would contain data pointers of the device
        raise NotImplementedError('CudaBuffer.__getbuffer__')

    def __getreadbuffer__(self, Py_ssize_t idx, void ** p):
        raise NotImplementedError('CudaBuffer.__getreadbuffer__')

    def __getwritebuffer__(self, Py_ssize_t idx, void ** p):
        raise NotImplementedError('CudaBuffer.__getwritebuffer__')


cdef class CudaHostBuffer(Buffer):
    """Device-accessible CPU memory created using cudaHostAlloc.

    To create a CudaHostBuffer instance, use

      <CudaDeviceManager instance>.allocate_host(<nbytes>)

    The memory is automatically freed when CudaHostBuffer instance is
    deleted.

    Note: after freeing CudaHostBuffer memory, the instance should be
    discarded as unusable.

    """

    def __cinit__(self):
        self._freed = True

    def __init__(self):
        raise TypeError("Do not call CudaHostBuffer's constructor directly,"
                        " use `allocate_cuda_host_buffer` function instead.")

    cdef void init_host(self, const shared_ptr[CCudaHostBuffer] & buffer):
        self.host_buffer = buffer
        self.init(< shared_ptr[CBuffer] > buffer)
        self._freed = False

    @property
    def size(self):
        if self._freed:
            return 0
        return self.host_buffer.get().size()


cdef class CudaBufferReader(NativeFile):
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
        """Read into new device buffer.

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
                                          < shared_ptr[CBuffer] * > & output))

        return pyarrow_wrap_cudabuffer(output)


cdef class CudaBufferWriter(NativeFile):
    """File interface for writing to CUDA buffers, with optional buffering.
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
        if isinstance(data, six.string_types):
            data = tobytes(data)

        cdef Buffer arrow_buffer = py_buffer(data)

        cdef const uint8_t * buf = arrow_buffer.buffer.get().data()
        cdef int64_t bufsize = len(arrow_buffer)
        with nogil:
            check_status(self.writer.WriteAt(position, buf, bufsize))

    def flush(self):
        """ Flush the buffer stream """
        with nogil:
            check_status(self.writer.Flush())

    def seek(self, int64_t position, int whence=0):
        # TODO: remove this method after NativeFile.seek supports
        # writeable files.
        cdef int64_t offset

        with nogil:
            if whence == 0:
                offset = position
            elif whence == 1:
                check_status(self.writer.Tell(& offset))
                offset = offset + position
            else:
                with gil:
                    raise ValueError("Invalid value of whence: {0}"
                                     .format(whence))
            check_status(self.writer.Seek(offset))
        return self.tell()

    def set_buffer_size(self, int64_t buffer_size):
        """Set CPU buffer size to limit calls to cudaMemcpy

        Parameters
        ----------
        buffer_size : int
          Specify the size of CPU buffer to allocate in bytes.
        """
        with nogil:
            check_status(self.writer.SetBufferSize(buffer_size))

    @property
    def buffer_size(self):
        """Returns size of host (CPU) buffer, 0 for unbuffered
        """
        return self.writer.buffer_size()

    @property
    def num_bytes_buffered(self):
        """Returns number of bytes buffered on host
        """
        return self.writer.num_bytes_buffered()

# Functions


def allocate_cuda_host_buffer(const int64_t size):
    """Allocate CUDA-accessible memory on CPU host

    Parameters
    ----------
    size : int
      Specify the number of bytes

    Returns
    -------
    dbuf : CudaHostBuffer
      the allocated device buffer
    """
    cdef shared_ptr[CCudaHostBuffer] buffer
    check_status(AllocateCudaHostBuffer(size, & buffer))
    return pyarrow_wrap_cudahostbuffer(buffer)


def cuda_serialize_record_batch(object batch, object ctx):
    """ Write record batch message to GPU device memory

    Parameters
    ----------
    batch : RecordBatch
      Specify record batch to write
    ctx : CudaContext
      Specify context to allocate device memory from

    Returns
    -------
    dbuf : CudaBuffer
      device buffer which contains the record batch message
    """
    cdef shared_ptr[CCudaBuffer] buffer
    cdef CRecordBatch * batch_ = pyarrow_unwrap_batch(batch).get()
    cdef CCudaContext * ctx_ = pyarrow_unwrap_cudacontext(ctx).get()
    check_status(CudaSerializeRecordBatch(batch_[0], ctx_, & buffer))
    return pyarrow_wrap_cudabuffer(buffer)


def cuda_read_message(object source, pool=None):
    """ Read Arrow IPC message located on GPU device

    Parameters
    ----------
    source : {CudaBuffer, CudaBufferReader}
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
    pool_ = maybe_unbox_memory_pool(pool)
    if not isinstance(source, CudaBufferReader):
        reader = CudaBufferReader(source)
    check_status(CudaReadMessage(reader.reader, pool_, & result.message))
    return result


def cuda_read_record_batch(object schema, object buffer, pool=None):
    """ Handles metadata on CUDA device

    Parameters
    ----------
    schema : Schema
      Specify schema for the record batch
    buffer :
      Specify device buffer containing the complete IPC message
    pool : {MemoryPool, None}
      Specify pool to use for allocating space for the metadata

    Returns
    -------
    batch : RecordBatch
      reconstructed record batch, with device pointers
    """
    cdef shared_ptr[CSchema] schema_ = pyarrow_unwrap_schema(schema)
    cdef shared_ptr[CCudaBuffer] buffer_ = pyarrow_unwrap_cudabuffer(buffer)
    pool_ = maybe_unbox_memory_pool(pool)
    cdef shared_ptr[CRecordBatch] batch
    check_status(CudaReadRecordBatch(schema_, buffer_, pool_, & batch))
    return pyarrow_wrap_batch(batch)

# Public API


cdef public api bint pyarrow_is_buffer(object buffer):
    return isinstance(buffer, Buffer)

cdef public api bint pyarrow_is_cudabuffer(object buffer):
    return isinstance(buffer, CudaBuffer)

cdef public api bint pyarrow_is_cudahostbuffer(object buffer):
    return isinstance(buffer, CudaHostBuffer)

cdef public api bint pyarrow_is_cudacontext(object ctx):
    return isinstance(ctx, CudaContext)

cdef public api object \
        pyarrow_wrap_cudabuffer(const shared_ptr[CCudaBuffer] & buf):
    cdef CudaBuffer result = CudaBuffer.__new__(CudaBuffer)
    result.init_cuda(buf)
    return result

cdef public api object \
        pyarrow_wrap_cudahostbuffer(const shared_ptr[CCudaHostBuffer] & buf):
    cdef CudaHostBuffer result = CudaHostBuffer.__new__(CudaHostBuffer)
    result.init_host(buf)
    return result

cdef public api object \
        pyarrow_wrap_cudacontext(const shared_ptr[CCudaContext] & ctx):
    cdef CudaContext result = CudaContext.__new__(CudaContext)
    result.init(ctx)
    return result

cdef public api bint pyarrow_is_cudaipcmemhandle(object handle):
    return isinstance(handle, CudaIpcMemHandle)

cdef public api object \
        pyarrow_wrap_cudaipcmemhandle(shared_ptr[CCudaIpcMemHandle] h):
    cdef CudaIpcMemHandle result = CudaIpcMemHandle.__new__(CudaIpcMemHandle)
    result.init(h)
    return result

cdef public api shared_ptr[CCudaIpcMemHandle] \
        pyarrow_unwrap_cudaipcmemhandle(object handle):
    cdef CudaIpcMemHandle handle_
    assert isinstance(handle, CudaIpcMemHandle)
    handle_ = < CudaIpcMemHandle > (handle)
    return handle_.handle

cdef public api shared_ptr[CCudaContext] \
        pyarrow_unwrap_cudacontext(object obj):
    cdef CudaContext ctx
    if pyarrow_is_cudacontext(obj):
        ctx = < CudaContext > (obj)
        return ctx.context
    return shared_ptr[CCudaContext]()

cdef public api shared_ptr[CCudaBuffer] pyarrow_unwrap_cudabuffer(object obj):
    cdef CudaBuffer buf
    if pyarrow_is_cudabuffer(obj):
        buf = < CudaBuffer > (obj)
        return buf.cuda_buffer
    return shared_ptr[CCudaBuffer]()

cdef public api shared_ptr[CCudaHostBuffer] \
        pyarrow_unwrap_cudahostbuffer(object obj):
    cdef CudaHostBuffer buf
    if pyarrow_is_cudahostbuffer(obj):
        buf = < CudaHostBuffer > (obj)
        return buf.host_buffer
    return shared_ptr[CCudaHostBuffer]()

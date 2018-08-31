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


from pyarrow.lib cimport *
from pyarrow.includes.libarrow_gpu cimport *
from pyarrow.lib import py_buffer, allocate_buffer

cdef class CudaDeviceManager:

    def __cinit__(self):
        check_status(CCudaDeviceManager.GetInstance(&self.manager))

    @property
    def num_devices(self):
        return self.manager.num_devices()

    def get_context(self, gpu_number = 0):
        cdef shared_ptr[CCudaContext] ctx
        check_status(self.manager.GetContext(gpu_number, &ctx))
        return pyarrow_wrap_cudacontext(ctx)

    def create_new_context(self, gpu_number): # for generic code use get_context
        cdef shared_ptr[CCudaContext] ctx
        check_status(self.manager.CreateNewContext(gpu_number, &ctx))
        return pyarrow_wrap_cudacontext(ctx)

    def allocate_host(self, nbytes):
        cdef shared_ptr[CCudaHostBuffer] buf
        check_status(self.manager.AllocateHost(nbytes, &buf))
        return pyarrow_wrap_cudahostbuffer(buf)
    
    def free_host(self, cudahostbuf): # TODO: get data and nbytes from cudahostbuf
        # check_status(self.manager.FreeHost(data, nbytes))
        pass
    
cdef class CudaContext:

    cdef void init(self, const shared_ptr[CCudaContext]& ctx):
        self.context = ctx

    def close(self):
        check_status(self.context.get().Close())

    def allocate(self, nbytes):
        cdef shared_ptr[CCudaBuffer] cudabuf
        check_status(self.context.get().Allocate(nbytes, &cudabuf))
        return pyarrow_wrap_cudabuffer(cudabuf)

    def open_ipc_buffer(self, ipc_handle):
        handle = pyarrow_unwrap_cudaipcmemhandle(ipc_handle)
        cdef shared_ptr[CCudaBuffer] cudabuf
        check_status(self.context.get().OpenIpcBuffer(handle.get()[0], &cudabuf))
        return pyarrow_wrap_cudabuffer(cudabuf)
    
    @property
    def bytes_allocated(self):
        return self.context.get().bytes_allocated() 

cdef class CudaIpcMemHandle:

    cdef void init(self, const shared_ptr[CCudaIpcMemHandle]& h):
        self.handle = h

    @staticmethod
    def from_buffer(opaque_handle):
        cdef shared_ptr[CCudaIpcMemHandle] handle
        #check_status(CCudaIpcMemHandle.FromBuffer(opaque_handle, &handle)) # TODO: const void* opaque_handle
        return pyarrow_wrap_cudaipcmemhandle(handle)

    def serialize(self, pool = None):
        pool_ = maybe_unbox_memory_pool(pool)
        cdef shared_ptr[CBuffer] buf
        check_status(self.handle.get().Serialize(pool_, &buf))
        return pyarrow_wrap_buffer(buf)


def py_cudabuffer(object obj):
    """Construct an Arrow GPU buffer from a Arrow Buffer or Python bytes
    object.  Host data will be copied to device memory.
    """
    if pyarrow_is_cudabuffer(obj):
        return obj # should it return a new CudaBuffer instance?
    cdef shared_ptr[CBuffer] buf
    if isinstance(obj, Buffer): # pyarrow_is_buffer is not reachable from here.
        buf = pyarrow_unwrap_buffer(obj)
    else:
        check_status(PyBuffer.FromPyObject(obj, &buf))
    cdef shared_ptr[CCudaBuffer] cbuf
    check_status(CCudaBuffer.FromBuffer(buf, &cbuf))  # this is WRONG!  
    return pyarrow_wrap_cudabuffer(cbuf)


def as_cudabuffer(object obj):
    if pyarrow_is_cudabuffer(obj):
        return obj
    return py_cudabuffer(obj)


cdef class CudaBuffer(Buffer):

    cdef void init_cuda(self, const shared_ptr[CCudaBuffer]& buffer):
        self.cuda_buffer = buffer
        self.init(<shared_ptr[CBuffer]> buffer)

    #TODO: CCudaBuffer(uint8_t* data, int64_t size, const shared_ptr[CCudaContext]& context, c_bool own_data = false, c_bool is_ipc = false)
    #TODO: CCudaBuffer(const shared_ptr[CCudaBuffer]& parent, const int64_t offset, const int64_t size)
        
    @staticmethod
    def from_buffer(buffer):
        buf = pyarrow_unwrap_buffer(buffer)
        cdef shared_ptr[CCudaBuffer] cbuf
        check_status(CCudaBuffer.FromBuffer(buf, &cbuf))
        return pyarrow_wrap_cudabuffer(cbuf)

    def copy_to_host(self, position, nbytes, MemoryPool memory_pool=None, c_bool resizable=False):
        buf = allocate_buffer(nbytes, memory_pool=memory_pool, resizable=resizable)
        cdef shared_ptr[CBuffer] buf_ = pyarrow_unwrap_buffer(buf)
        check_status(self.cuda_buffer.get().CopyToHost(position, nbytes, buf_.get().mutable_data()))
        return buf
    
    def copy_from_host(self, position, data, nbytes):
        buf = data if isinstance(data, Buffer) else py_buffer(data)
        cdef shared_ptr[CBuffer] buf_ = pyarrow_unwrap_buffer(buf)
        check_status(self.cuda_buffer.get().CopyFromHost(position, buf_.get().data(), nbytes))
    
    def export_for_ipc(self):
        cdef shared_ptr[CCudaIpcMemHandle] handle
        check_status(self.cuda_buffer.get().ExportForIpc(&handle))
        return pyarrow_wrap_cudaipcmemhandle(handle)
    
    @property
    def context(self):
        return pyarrow_wrap_cudacontext(self.cuda_buffer.get().context())

cdef class CudaHostBuffer(Buffer):

    cdef void init_host(self, const shared_ptr[CCudaHostBuffer]& buffer):
        self.init(<shared_ptr[CBuffer]> buffer)
    
cdef class CudaBufferReader(NativeFile):

    def __cinit__(self, object obj):
        if isinstance(obj, CudaBuffer):
            self.buffer = obj
        else:
            self.buffer = py_cudabuffer(obj)
        self.reader = new CCudaBufferReader(self.buffer.buffer)
        self.rd_file.reset(self.reader)
        self.is_readable = True
        self.closed = False

    #TODO: CStatus Read(int64_t nbytes, int64_t* bytes_read, void* buffer)
    #TODO: CStatus Read(int64_t nbytes, shared_ptr[CBuffer]* out)

        
cdef class CudaBufferWriter(NativeFile):

    def __cinit__(self, CudaBuffer buffer):
        self.writer = new CCudaBufferWriter(buffer.cuda_buffer)
        self.wr_file.reset(self.writer)
        self.is_writable = True
        self.closed = False
 
    #TODO: CStatus Close()
    #TODO: CStatus Flush()
    #TODO: CStatus Seek(int64_t position)
    #TODO: CStatus Write(const void* data, int64_t nbytes)
    #TODO: CStatus WriteAt(int64_t position, const void* data, int64_t nbytes)
    #TODO: CStatus Tell(int64_t* position) const

    def set_buffer_size(self, buffer_size):
        check_status(self.writer.SetBufferSize(buffer_size))
    
    @property
    def buffer_size(self):
        return self.writer.buffer_size() 

    @property
    def num_bytes_buffered(self):
        return self.writer.num_bytes_buffered() 

# Functions

def allocate_cuda_host_buffer(const int64_t size):
    cdef shared_ptr[CCudaHostBuffer] buffer
    check_status(AllocateCudaHostBuffer(size, &buffer))
    return pyarrow_wrap_cudahostbuffer(buffer)

def cuda_serialize_record_batch(object batch, object ctx):
    cdef shared_ptr[CCudaBuffer] buffer
    cdef CRecordBatch* batch_ = pyarrow_unwrap_batch(batch).get()
    cdef CCudaContext* ctx_ = pyarrow_unwrap_cudacontext(ctx).get()
    check_status(CudaSerializeRecordBatch(batch_[0], ctx_, &buffer))
    return pyarrow_wrap_cudabuffer(buffer)

def cuda_read_message(object source, pool = None):
    cdef:
        Message result = Message.__new__(Message)
    pool_ = maybe_unbox_memory_pool(pool)
    reader = CudaBufferReader(source)
    check_status(CudaReadMessage(reader.reader, pool_, &result.message))
    return result

def cuda_read_record_batch(object schema, object buffer, pool = None):
    cdef shared_ptr[CSchema] schema_ = pyarrow_unwrap_schema(schema)
    cdef shared_ptr[CCudaBuffer] buffer_ = pyarrow_unwrap_cudabuffer(buffer)
    pool_ = maybe_unbox_memory_pool(pool)
    cdef shared_ptr[CRecordBatch] batch
    check_status(CudaReadRecordBatch(schema_, buffer_, pool_, &batch))
    return pyarrow_wrap_batch(batch)

# Public API

cdef public api bint pyarrow_is_cudabuffer(object buffer):
    return isinstance(buffer, CudaBuffer)

cdef public api bint pyarrow_is_cudacontext(object ctx):
    return isinstance(ctx, CudaContext)

cdef public api object pyarrow_wrap_cudabuffer(const shared_ptr[CCudaBuffer]& buf):
    cdef CudaBuffer result = CudaBuffer.__new__(CudaBuffer)
    result.init_cuda(buf)
    return result

cdef public api object pyarrow_wrap_cudahostbuffer(const shared_ptr[CCudaHostBuffer]& buf):
    cdef CudaHostBuffer result = CudaHostBuffer.__new__(CudaHostBuffer)
    result.init_host(buf)
    return result

cdef public api object pyarrow_wrap_cudacontext(const shared_ptr[CCudaContext]& ctx):
    cdef CudaContext result = CudaContext.__new__(CudaContext)
    result.init(ctx)
    return result

cdef public api bint pyarrow_is_cudaipcmemhandle(object handle):
    return isinstance(handle, CudaIpcMemHandle)

cdef public api object pyarrow_wrap_cudaipcmemhandle(const shared_ptr[CCudaIpcMemHandle]& h):
    cdef CudaIpcMemHandle result = CudaIpcMemHandle.__new__(CudaIpcMemHandle)
    result.init(h)
    return result

cdef public api shared_ptr[CCudaIpcMemHandle] pyarrow_unwrap_cudaipcmemhandle(object handle):
    cdef CudaIpcMemHandle handle_
    if pyarrow_is_cudaipcmemhandle(handle):
        handle_ = <CudaIpcMemHandle>(handle)
        return handle_.handle
    return shared_ptr[CCudaIpcMemHandle]()

cdef public api shared_ptr[CCudaContext] pyarrow_unwrap_cudacontext(object obj):
    cdef CudaContext ctx
    if pyarrow_is_cudacontext(obj):
        ctx = <CudaContext>(obj)
        return ctx.context
    return shared_ptr[CCudaContext]()

cdef public api shared_ptr[CCudaBuffer] pyarrow_unwrap_cudabuffer(object obj):
    cdef CudaBuffer buf
    if pyarrow_is_cudabuffer(obj):
        buf = <CudaBuffer>(obj)
        return buf.cuda_buffer
    return shared_ptr[CCudaBuffer]()

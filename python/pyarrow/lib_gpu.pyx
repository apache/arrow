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
from libcpp.cast cimport dynamic_cast, static_cast

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

    def allocate_host(self, nbytes): # TODO: CCudaHostBuffer
        #cdef shared_ptr[CCudaHostBuffer] buf
        #check_status(self.manager.AllocateHost(nbytes, &buf))
        #return pyarrow_wrap_cudahostbuffer(buf)
        pass
    
    def free_host(self, cudahostbuf): # TODO: get data and nbytes from cudahostbuf
        # check_status(self.manager.FreeHost(data, nbytes))
        pass
    
cdef class CudaContext:

    cdef void init(self, shared_ptr[CCudaContext]& ctx):
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

    cdef void init(self, shared_ptr[CCudaIpcMemHandle]& h):
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
        self.init(<shared_ptr[CBuffer]> buffer)

    
# Public API

cdef public api bint pyarrow_is_cudabuffer(object buffer):
    return isinstance(buffer, CudaBuffer)

cdef public api object pyarrow_wrap_cudabuffer(const shared_ptr[CCudaBuffer]& buf):
    cdef CudaBuffer result = CudaBuffer.__new__(CudaBuffer)
    result.init_cuda(buf)
    return result

cdef public api object pyarrow_wrap_cudacontext(shared_ptr[CCudaContext]& ctx):
    cdef CudaContext result = CudaContext.__new__(CudaContext)
    result.init(ctx)
    return result

cdef public api bint pyarrow_is_cudaipcmemhandle(object handle):
    return isinstance(handle, CudaIpcMemHandle)

cdef public api object pyarrow_wrap_cudaipcmemhandle(shared_ptr[CCudaIpcMemHandle]& h):
    cdef CudaIpcMemHandle result = CudaIpcMemHandle.__new__(CudaIpcMemHandle)
    result.init(h)
    return result

cdef public api shared_ptr[CCudaIpcMemHandle] pyarrow_unwrap_cudaipcmemhandle(object handle):
    cdef CudaIpcMemHandle handle_
    if pyarrow_is_cudaipcmemhandle(handle):
        handle_ = <CudaIpcMemHandle>(handle)
        return handle_.handle
    return shared_ptr[CCudaIpcMemHandle]()

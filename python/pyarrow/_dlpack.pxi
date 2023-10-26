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

from libc.stdlib cimport malloc, free

cimport cpython
from cpython.pycapsule cimport PyCapsule_New
from cython import sizeof


ctypedef enum DLDeviceType:
    kDLCPU = 1
    kDLCUDA = 2
    kDLCUDAHost = 3
    kDLOpenCL = 4
    kDLVulkan = 7
    kDLMetal = 8
    kDLVPI = 9
    kDLROCM = 10
    kDLROCMHost = 11
    kDLExtDev = 12
    kDLCUDAManaged = 13
    kDLOneAPI = 14
    kDLWebGPU = 15
    kDLHexagon = 16

ctypedef struct DLDevice:
    DLDeviceType device_type
    int32_t device_id

ctypedef enum DLDataTypeCode:
    kDLInt = 0
    kDLUInt = 1
    kDLFloat = 2
    kDLOpaqueHandle = 3
    kDLBfloat = 4
    kDLComplex = 5
    kDLBool = 6

ctypedef struct DLDataType:
    uint8_t code
    uint8_t bits
    uint16_t lanes

ctypedef struct DLTensor:
    void *data
    DLDevice device
    int32_t ndim
    DLDataType dtype
    int64_t *shape
    int64_t *strides
    uint64_t byte_offset

ctypedef struct DLManagedTensor:
    DLTensor dl_tensor
    void *manager_ctx
    void (*deleter)(DLManagedTensor *)


cdef void pycapsule_deleter(object dltensor):
    cdef DLManagedTensor* dlm_tensor
    if cpython.PyCapsule_IsValid(dltensor, 'dltensor'):
        dlm_tensor = <DLManagedTensor*>cpython.PyCapsule_GetPointer(
            dltensor, 'dltensor')
        dlm_tensor.deleter(dlm_tensor)


cdef void deleter(DLManagedTensor* tensor) with gil:
    if tensor.manager_ctx is NULL:
        return
    free(tensor.dl_tensor.shape)
    cpython.Py_DECREF(<Array>tensor.manager_ctx)
    tensor.manager_ctx = NULL
    free(tensor)


cpdef object to_dlpack(Array arr) except +:
    cdef DLManagedTensor* dlm_tensor = <DLManagedTensor*> malloc(sizeof(DLManagedTensor))

    cdef DLTensor* dl_tensor = &dlm_tensor.dl_tensor
    dl_tensor.data = <void*> arr.buffers()[1].address
    dl_tensor.ndim = 1
    cdef int64_t* shape = <int64_t*> malloc(sizeof(int64_t))
    shape[0] = len(arr)
    dl_tensor.shape = shape

    dl_tensor.strides = NULL
    dl_tensor.byte_offset = 0

    cdef DLDevice* device = &dl_tensor.device
    device.device_type = kDLCPU
    device.device_id = 0

    cdef DLDataType* dtype = &dl_tensor.dtype
    if arr.type in [uint8(), uint16(), uint32(), uint64()]:
        dtype.code = kDLUInt
    elif arr.type in [int8(), int16(), int32(), int64()]:
        dtype.code = kDLInt
    elif arr.type in [float16(), float32(), float64()]:
        dtype.code = kDLFloat
    elif arr.type == bool_():
        dtype.code = kDLBool
    else:
        raise ValueError(f'Unsupported dtype {arr.type}')
    dtype.lanes = <uint16_t>1
    dtype.bits = <uint8_t>arr.type.bit_width

    dlm_tensor.manager_ctx = <void*>arr
    cpython.Py_INCREF(arr)
    dlm_tensor.deleter = deleter

    return PyCapsule_New(dlm_tensor, 'dltensor', pycapsule_deleter)

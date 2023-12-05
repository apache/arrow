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


cdef void pycapsule_deleter(object dltensor) noexcept:
    cdef DLManagedTensor* dlm_tensor
    cdef PyObject* err_type
    cdef PyObject* err_value
    cdef PyObject* err_traceback

    # Do nothing if the capsule has been consumed
    if cpython.PyCapsule_IsValid(dltensor, "used_dltensor"):
        return

    # An exception may be in-flight, we must save it in case
    # we create another one
    cpython.PyErr_Fetch(&err_type, &err_value, &err_traceback)

    if cpython.PyCapsule_IsValid(dltensor, 'dltensor'):
        dlm_tensor = <DLManagedTensor*>cpython.PyCapsule_GetPointer(
            dltensor, 'dltensor')
        dlm_tensor.deleter(dlm_tensor)
    else:
        cpython.PyErr_WriteUnraisable(dltensor)

    # Set the error indicator from err_type, err_value, err_traceback
    cpython.PyErr_Restore(err_type, err_value, err_traceback)


cpdef object to_dlpack(Array arr) except *:

    cdef CResult[DLManagedTensor*] c_dlm_tensor
    c_dlm_tensor = ExportToDLPack(pyarrow_unwrap_array(arr))
    dlm_tensor = GetResultValue(c_dlm_tensor)

    return PyCapsule_New(dlm_tensor, 'dltensor', pycapsule_deleter)

cpdef object dlpack_device(Array arr) except *:

    cdef CResult[DLDevice] c_device
    c_device = ExportDevice(pyarrow_unwrap_array(arr))
    device = GetResultValue(c_device)

    return (device.device_type, device.device_id)

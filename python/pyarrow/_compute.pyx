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

# cython: language_level = 3

from pyarrow.lib cimport (
    Array,
    wrap_datum,
    check_status,
    ChunkedArray,
    ScalarValue
)
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.common cimport *

from pyarrow.compat import frombytes, tobytes


cdef wrap_function(const shared_ptr[CFunction]& sp_func):
    if sp_func.get() == NULL:
        raise ValueError('Function was NULL')

    cdef Function func = Function.__new__(Function)
    func.init(sp_func)
    return func



cdef class Function:
    """
    The base class for all Arrow arrays.
    """
    cdef:
        shared_ptr[CFunction] sp_func
        const CFunction* func

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly"
                        .format(self.__class__.__name__))

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        self.sp_func = sp_func
        self.func = sp_func.get()

    @property
    def num_kernels(self):
        return self.func.num_kernels()

    def call(self, args):
        cdef:
            const CFunctionOptions* c_options = NULL
            vector[CDatum] c_args
            CDatum result

        _pack_compute_args(args, &c_args)

        with nogil:
            result = GetResultValue(self.func.Execute(c_args, c_options))

        return wrap_datum(result)


cdef _pack_compute_args(object values, vector[CDatum]* out):
    for val in values:
        if isinstance(val, Array):
            out.push_back(CDatum((<Array> val).sp_array))
        elif isinstance(val, ChunkedArray):
            out.push_back(CDatum((<ChunkedArray> val).sp_chunked_array))
        elif isinstance(val, ScalarValue):
            out.push_back(CDatum((<ScalarValue> val).sp_scalar))
        else:
            raise TypeError(type(val))


cdef class FunctionRegistry:
    cdef:
        CFunctionRegistry* registry

    def __init__(self):
        self.registry = GetFunctionRegistry()

    def list_functions(self):
        cdef vector[c_string] names = self.registry.GetFunctionNames()
        return [frombytes(name) for name in names]

    def get_function(self, name):
        cdef:
            c_string c_name = tobytes(name)
            shared_ptr[CFunction] func
        with nogil:
            func = GetResultValue(self.registry.GetFunction(c_name))
        return wrap_function(func)


cdef FunctionRegistry _global_func_registry = FunctionRegistry()


def call_function(name, args):
    func = _global_func_registry.get_function(name)
    return func.call(args)


def sum(array):
    """
    Sum the values in a numerical (chunked) array.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray

    Returns
    -------
    sum : pyarrow.Scalar
    """
    return call_function('sum', [array])

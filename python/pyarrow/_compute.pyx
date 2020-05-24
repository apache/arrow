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


cdef wrap_scalar_function(const shared_ptr[CFunction]& sp_func):
    cdef ScalarFunction func = ScalarFunction.__new__(ScalarFunction)
    func.init(sp_func)
    return func


cdef wrap_vector_function(const shared_ptr[CFunction]& sp_func):
    cdef VectorFunction func = VectorFunction.__new__(VectorFunction)
    func.init(sp_func)
    return func


cdef wrap_scalar_aggregate_function(const shared_ptr[CFunction]& sp_func):
    cdef ScalarAggregateFunction func = (
        ScalarAggregateFunction.__new__(ScalarAggregateFunction)
    )
    func.init(sp_func)
    return func


cdef wrap_function(const shared_ptr[CFunction]& sp_func):
    if sp_func.get() == NULL:
        raise ValueError('Function was NULL')

    cdef FunctionKind c_kind = sp_func.get().kind()
    if c_kind == FunctionKind_SCALAR:
        return wrap_scalar_function(sp_func)
    elif c_kind == FunctionKind_VECTOR:
        return wrap_vector_function(sp_func)
    elif c_kind == FunctionKind_SCALAR_AGGREGATE:
        return wrap_scalar_aggregate_function(sp_func)
    else:
        raise NotImplementedError("Unknown Function::Kind")


cdef wrap_scalar_kernel(const CScalarKernel* c_kernel):
    if c_kernel == NULL:
        raise ValueError('Kernel was NULL')
    cdef ScalarKernel kernel = ScalarKernel.__new__(ScalarKernel)
    kernel.init(c_kernel)
    return kernel


cdef wrap_vector_kernel(const CVectorKernel* c_kernel):
    if c_kernel == NULL:
        raise ValueError('Kernel was NULL')
    cdef VectorKernel kernel = VectorKernel.__new__(VectorKernel)
    kernel.init(c_kernel)
    return kernel


cdef wrap_scalar_aggregate_kernel(const CScalarAggregateKernel* c_kernel):
    if c_kernel == NULL:
        raise ValueError('Kernel was NULL')
    cdef ScalarAggregateKernel kernel = (
        ScalarAggregateKernel.__new__(ScalarAggregateKernel)
    )
    kernel.init(c_kernel)
    return kernel


cdef class Kernel:

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly"
                        .format(self.__class__.__name__))


cdef class ScalarKernel(Kernel):
    cdef:
        const CScalarKernel* kernel

    cdef void init(self, const CScalarKernel* kernel) except *:
        self.kernel = kernel

    def __repr__(self):
        return ("ScalarKernel<{}>"
                .format(frombytes(self.kernel.signature.get().ToString())))


cdef class VectorKernel(Kernel):
    cdef:
        const CVectorKernel* kernel

    cdef void init(self, const CVectorKernel* kernel) except *:
        self.kernel = kernel

    def __repr__(self):
        return ("VectorKernel<{}>"
                .format(frombytes(self.kernel.signature.get().ToString())))


cdef class ScalarAggregateKernel(Kernel):
    cdef:
        const CScalarAggregateKernel* kernel

    cdef void init(self, const CScalarAggregateKernel* kernel) except *:
        self.kernel = kernel

    def __repr__(self):
        return ("ScalarAggregateKernel<{}>"
                .format(frombytes(self.kernel.signature.get().ToString())))


cdef class Function:
    cdef:
        shared_ptr[CFunction] sp_func
        CFunction* base_func

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly"
                        .format(self.__class__.__name__))

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        self.sp_func = sp_func
        self.base_func = sp_func.get()

    def __repr__(self):
        return """arrow.compute.Function
kind: {}
num_kernels: {}
""".format(self.kind, self.num_kernels)

    @property
    def kind(self):
        cdef FunctionKind c_kind = self.base_func.kind()
        if c_kind == FunctionKind_SCALAR:
            return 'scalar'
        elif c_kind == FunctionKind_VECTOR:
            return 'vector'
        elif c_kind == FunctionKind_SCALAR_AGGREGATE:
            return 'scalar_aggregate'
        else:
            raise NotImplementedError("Unknown Function::Kind")

    @property
    def num_kernels(self):
        return self.base_func.num_kernels()

    def call(self, args, options=None):
        cdef:
            const CFunctionOptions* c_options = NULL
            vector[CDatum] c_args
            CDatum result

        _pack_compute_args(args, &c_args)

        if isinstance(options, FunctionOptions):
            c_options = (<FunctionOptions> options).options()

        with nogil:
            result = GetResultValue(self.base_func.Execute(c_args, c_options))

        return wrap_datum(result)


cdef class ScalarFunction(Function):
    cdef:
        const CScalarFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CScalarFunction*> sp_func.get()

    def list_kernels(self):
        cdef vector[const CScalarKernel*] kernels = self.func.kernels()
        return [wrap_scalar_kernel(k) for k in kernels]


cdef class VectorFunction(Function):
    cdef:
        const CVectorFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CVectorFunction*> sp_func.get()

    def list_kernels(self):
        cdef vector[const CVectorKernel*] kernels = self.func.kernels()
        return [wrap_vector_kernel(k) for k in kernels]


cdef class ScalarAggregateFunction(Function):
    cdef:
        const CScalarAggregateFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CScalarAggregateFunction*> sp_func.get()

    def list_kernels(self):
        cdef vector[const CScalarAggregateKernel*] kernels = (
            self.func.kernels()
        )
        return [wrap_scalar_aggregate_kernel(k) for k in kernels]


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


def function_registry():
    return _global_func_registry


def call_function(name, args, options=None):
    func = _global_func_registry.get_function(name)
    return func.call(args, options=options)


cdef class FunctionOptions:

    cdef const CFunctionOptions* options(self) except NULL:
        raise NotImplementedError("Unimplemented base options")


cdef class CastOptions(FunctionOptions):
    cdef:
        CCastOptions cast_options

    @staticmethod
    def safe():
        cdef CastOptions options = CastOptions()
        options.cast_options = CCastOptions.Safe()

    @staticmethod
    def unsafe():
        cdef CastOptions options = CastOptions()
        options.cast_options = CCastOptions.Unsafe()

    cdef const CFunctionOptions* options(self) except NULL:
        return &self.cast_options


cdef class FilterOptions(FunctionOptions):
    cdef:
        CFilterOptions filter_options

    def __init__(self, null_selection_behavior='drop'):
        if null_selection_behavior == 'drop':
            self.filter_options.null_selection_behavior = (
                CFilterNullSelectionBehavior_DROP
            )
        elif null_selection_behavior == 'emit_null':
            self.filter_options.null_selection_behavior = (
                CFilterNullSelectionBehavior_EMIT_NULL
            )
        else:
            raise ValueError(
                '"{}" is not a valid null_selection_behavior'.format(
                    null_selection_behavior)
            )

    cdef const CFunctionOptions* options(self) except NULL:
        return &self.filter_options

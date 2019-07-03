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


cdef class Tensor:
    """
    A n-dimensional array a.k.a Tensor.
    """

    def __init__(self):
        raise TypeError("Do not call Tensor's constructor directly, use one "
                        "of the `pyarrow.Tensor.from_*` functions instead.")

    cdef void init(self, const shared_ptr[CTensor]& sp_tensor):
        self.sp_tensor = sp_tensor
        self.tp = sp_tensor.get()
        self.type = pyarrow_wrap_data_type(self.tp.type())

    def __repr__(self):
        return """<pyarrow.Tensor>
type: {0.type}
shape: {0.shape}
strides: {0.strides}""".format(self)

    @staticmethod
    def from_numpy(obj, dim_names=None):
        cdef:
            vector[c_string] c_dim_names
            shared_ptr[CTensor] ctensor

        if dim_names is not None:
            for x in dim_names:
                c_dim_names.push_back(tobytes(x))

        check_status(NdarrayToTensor(c_default_memory_pool(), obj,
                                     c_dim_names, &ctensor))
        return pyarrow_wrap_tensor(ctensor)

    def to_numpy(self):
        """
        Convert arrow::Tensor to numpy.ndarray with zero copy
        """
        cdef PyObject* out

        check_status(TensorToNdarray(self.sp_tensor, self, &out))
        return PyObject_to_object(out)

    def equals(self, Tensor other):
        """
        Return true if the tensors contains exactly equal data
        """
        return self.tp.Equals(deref(other.tp))

    def __eq__(self, other):
        if isinstance(other, Tensor):
            return self.equals(other)
        else:
            return NotImplemented

    def dim_name(self, i):
        return frombytes(self.tp.dim_name(i))

    @property
    def dim_names(self):
        return [frombytes(x) for x in tuple(self.tp.dim_names())]

    @property
    def is_mutable(self):
        return self.tp.is_mutable()

    @property
    def is_contiguous(self):
        return self.tp.is_contiguous()

    @property
    def ndim(self):
        return self.tp.ndim()

    @property
    def size(self):
        return self.tp.size()

    @property
    def shape(self):
        # Cython knows how to convert a vector[T] to a Python list
        return tuple(self.tp.shape())

    @property
    def strides(self):
        return tuple(self.tp.strides())

    def __getbuffer__(self, cp.Py_buffer* buffer, int flags):
        buffer.buf = <char *> self.tp.data().get().data()
        pep3118_format = self.type.pep3118_format
        if pep3118_format is None:
            raise NotImplementedError("type %s not supported for buffer "
                                      "protocol" % (self.type,))
        buffer.format = pep3118_format
        buffer.itemsize = self.type.bit_width // 8
        buffer.internal = NULL
        buffer.len = self.tp.size() * buffer.itemsize
        buffer.ndim = self.tp.ndim()
        buffer.obj = self
        if self.tp.is_mutable():
            buffer.readonly = 0
        else:
            buffer.readonly = 1
        # NOTE: This assumes Py_ssize_t == int64_t, and that the shape
        # and strides arrays lifetime is tied to the tensor's
        buffer.shape = <Py_ssize_t *> &self.tp.shape()[0]
        buffer.strides = <Py_ssize_t *> &self.tp.strides()[0]
        buffer.suboffsets = NULL


cdef class SparseTensorCOO:
    """
    A sparse COO tensor.
    """

    def __init__(self):
        raise TypeError("Do not call SparseTensorCOO's constructor directly, "
                        "use one of the `pyarrow.SparseTensorCOO.from_*` "
                        "functions instead.")

    cdef void init(self, const shared_ptr[CSparseTensorCOO]& sp_sparse_tensor):
        self.sp_sparse_tensor = sp_sparse_tensor
        self.stp = sp_sparse_tensor.get()
        self.type = pyarrow_wrap_data_type(self.stp.type())

    def __repr__(self):
        return """<pyarrow.SparseTensorCOO>
type: {0.type}
shape: {0.shape}""".format(self)

    @classmethod
    def from_dense_numpy(cls, obj, dim_names=None):
        """
        Convert numpy.ndarray to arrow::SparseTensorCOO
        """
        return cls.from_tensor(Tensor.from_numpy(obj, dim_names=dim_names))

    @staticmethod
    def from_numpy(data, coords, shape, dim_names=None):
        """
        Create arrow::SparseTensorCOO from numpy.ndarrays
        """
        cdef shared_ptr[CSparseTensorCOO] csparse_tensor
        cdef vector[int64_t] c_shape
        cdef vector[c_string] c_dim_names

        for x in shape:
            c_shape.push_back(x)
        if dim_names is not None:
            for x in dim_names:
                c_dim_names.push_back(tobytes(x))

        # Enforce precondition for SparseTensorCOO indices
        coords = np.require(coords, dtype='i8', requirements='F')
        if coords.ndim != 2:
            raise ValueError("Expected 2-dimensional array for "
                             "SparseTensorCOO indices")

        check_status(NdarraysToSparseTensorCOO(c_default_memory_pool(),
                     data, coords, c_shape, c_dim_names, &csparse_tensor))
        return pyarrow_wrap_sparse_tensor_coo(csparse_tensor)

    @staticmethod
    def from_tensor(obj):
        """
        Convert arrow::Tensor to arrow::SparseTensorCOO
        """
        cdef shared_ptr[CSparseTensorCOO] csparse_tensor
        cdef shared_ptr[CTensor] ctensor = pyarrow_unwrap_tensor(obj)

        with nogil:
            check_status(TensorToSparseTensorCOO(ctensor, &csparse_tensor))

        return pyarrow_wrap_sparse_tensor_coo(csparse_tensor)

    def to_numpy(self):
        """
        Convert arrow::SparseTensorCOO to numpy.ndarrays with zero copy
        """
        cdef PyObject* out_data
        cdef PyObject* out_coords

        check_status(SparseTensorCOOToNdarray(self.sp_sparse_tensor, self,
                                              &out_data, &out_coords))
        return PyObject_to_object(out_data), PyObject_to_object(out_coords)

    def equals(self, SparseTensorCOO other):
        """
        Return true if sparse tensors contains exactly equal data
        """
        return self.stp.Equals(deref(other.stp))

    def __eq__(self, other):
        if isinstance(other, SparseTensorCOO):
            return self.equals(other)
        else:
            return NotImplemented

    @property
    def is_mutable(self):
        return self.stp.is_mutable()

    @property
    def ndim(self):
        return self.stp.ndim()

    @property
    def shape(self):
        # Cython knows how to convert a vector[T] to a Python list
        return tuple(self.stp.shape())

    @property
    def size(self):
        return self.stp.size()

    def dim_name(self, i):
        return frombytes(self.stp.dim_name(i))

    @property
    def dim_names(self):
        return [frombytes(x) for x in tuple(self.stp.dim_names())]

    @property
    def non_zero_length(self):
        return self.stp.non_zero_length()


cdef class SparseTensorCSR:
    """
    A sparse CSR tensor.
    """

    def __init__(self):
        raise TypeError("Do not call SparseTensorCSR's constructor directly, "
                        "use one of the `pyarrow.SparseTensorCSR.from_*` "
                        "functions instead.")

    cdef void init(self, const shared_ptr[CSparseTensorCSR]& sp_sparse_tensor):
        self.sp_sparse_tensor = sp_sparse_tensor
        self.stp = sp_sparse_tensor.get()
        self.type = pyarrow_wrap_data_type(self.stp.type())

    def __repr__(self):
        return """<pyarrow.SparseTensorCSR>
type: {0.type}
shape: {0.shape}""".format(self)

    @classmethod
    def from_dense_numpy(cls, obj, dim_names=None):
        """
        Convert numpy.ndarray to arrow::SparseTensorCSR
        """
        return cls.from_tensor(Tensor.from_numpy(obj, dim_names=dim_names))

    @staticmethod
    def from_numpy(data, indptr, indices, shape, dim_names=None):
        """
        Create arrow::SparseTensorCSR from numpy.ndarrays
        """
        cdef shared_ptr[CSparseTensorCSR] csparse_tensor
        cdef vector[int64_t] c_shape
        cdef vector[c_string] c_dim_names

        for x in shape:
            c_shape.push_back(x)
        if dim_names is not None:
            for x in dim_names:
                c_dim_names.push_back(tobytes(x))

        # Enforce precondition for SparseTensorCSR indices
        indptr = np.require(indptr, dtype='i8')
        indices = np.require(indices, dtype='i8')
        if indptr.ndim != 1:
            raise ValueError("Expected 1-dimensional array for "
                             "SparseTensorCSR indptr")
        if indices.ndim != 1:
            raise ValueError("Expected 1-dimensional array for "
                             "SparseTensorCSR indices")

        check_status(NdarraysToSparseTensorCSR(c_default_memory_pool(),
                     data, indptr, indices, c_shape, c_dim_names,
                     &csparse_tensor))
        return pyarrow_wrap_sparse_tensor_csr(csparse_tensor)

    @staticmethod
    def from_tensor(obj):
        """
        Convert arrow::Tensor to arrow::SparseTensorCSR
        """
        cdef shared_ptr[CSparseTensorCSR] csparse_tensor
        cdef shared_ptr[CTensor] ctensor = pyarrow_unwrap_tensor(obj)

        with nogil:
            check_status(TensorToSparseTensorCSR(ctensor, &csparse_tensor))

        return pyarrow_wrap_sparse_tensor_csr(csparse_tensor)

    def to_numpy(self):
        """
        Convert arrow::SparseTensorCSR to numpy.ndarrays with zero copy
        """
        cdef PyObject* out_data
        cdef PyObject* out_indptr
        cdef PyObject* out_indices

        check_status(SparseTensorCSRToNdarray(self.sp_sparse_tensor, self,
                     &out_data, &out_indptr, &out_indices))
        return (PyObject_to_object(out_data), PyObject_to_object(out_indptr),
                PyObject_to_object(out_indices))

    def equals(self, SparseTensorCSR other):
        """
        Return true if sparse tensors contains exactly equal data
        """
        return self.stp.Equals(deref(other.stp))

    def __eq__(self, other):
        if isinstance(other, SparseTensorCSR):
            return self.equals(other)
        else:
            return NotImplemented

    @property
    def is_mutable(self):
        return self.stp.is_mutable()

    @property
    def ndim(self):
        return self.stp.ndim()

    @property
    def shape(self):
        # Cython knows how to convert a vector[T] to a Python list
        return tuple(self.stp.shape())

    @property
    def size(self):
        return self.stp.size()

    def dim_name(self, i):
        return frombytes(self.stp.dim_name(i))

    @property
    def dim_names(self):
        return [frombytes(x) for x in tuple(self.stp.dim_names())]

    @property
    def non_zero_length(self):
        return self.stp.non_zero_length()

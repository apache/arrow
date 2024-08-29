.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. currentmodule:: pyarrow
.. cpp:namespace:: arrow

.. _extending:

Using pyarrow from C++ and Cython Code
======================================

pyarrow provides both a Cython and C++ API, allowing your own native code
to interact with pyarrow objects.

C++ API
-------

.. default-domain:: cpp

The Arrow C++ and PyArrow C++ header files are bundled with a pyarrow installation.
To get the absolute path to this directory (like ``numpy.get_include()``), use:

.. code-block:: python

   import pyarrow as pa
   pa.get_include()

Assuming the path above is on your compiler's include path, the pyarrow API
can be included using the following directive:

.. code-block:: cpp

   #include <arrow/python/pyarrow.h>

This will not include other parts of the Arrow API, which you will need
to include yourself (for example ``arrow/api.h``).

When building C extensions that use the Arrow C++ libraries, you must add
appropriate linker flags. We have provided functions ``pa.get_libraries``
and ``pa.get_library_dirs`` which return a list of library names and
likely library install locations (if you installed pyarrow with pip or
conda). These must be included when declaring your C extensions with
setuptools (see below).

.. note::

   The PyArrow-specific C++ code is now a part of the PyArrow source tree
   and not Arrow C++. That means the header files and ``arrow_python`` library
   are not necessarily installed in the same location as that of Arrow C++ and
   will no longer be automatically findable by CMake.

Initializing the API
~~~~~~~~~~~~~~~~~~~~

.. function:: int import_pyarrow()

   Initialize inner pointers of the pyarrow API.  On success, 0 is
   returned.  Otherwise, -1 is returned and a Python exception is set.

   It is mandatory to call this function before calling any other function
   in the pyarrow C++ API.  Failing to do so will likely lead to crashes.

Wrapping and Unwrapping
~~~~~~~~~~~~~~~~~~~~~~~

pyarrow provides the following functions to go back and forth between
Python wrappers (as exposed by the pyarrow Python API) and the underlying
C++ objects.

.. function:: bool arrow::py::is_array(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Array` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Array` instance.

.. function:: bool arrow::py::is_batch(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`RecordBatch` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.RecordBatch` instance.

.. function:: bool arrow::py::is_buffer(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Buffer` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Buffer` instance.

.. function:: bool arrow::py::is_data_type(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`DataType` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.DataType` instance.

.. function:: bool arrow::py::is_field(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Field` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Field` instance.

.. function:: bool arrow::py::is_scalar(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Scalar` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Scalar` instance.

.. function:: bool arrow::py::is_schema(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Schema` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Schema` instance.

.. function:: bool arrow::py::is_table(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Table` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Table` instance.

.. function:: bool arrow::py::is_tensor(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Tensor` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Tensor` instance.

.. function:: bool arrow::py::is_sparse_coo_tensor(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :type:`SparseCOOTensor` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.SparseCOOTensor` instance.

.. function:: bool arrow::py::is_sparse_csc_matrix(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :type:`SparseCSCMatrix` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.SparseCSCMatrix` instance.

.. function:: bool arrow::py::is_sparse_csf_tensor(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :type:`SparseCSFTensor` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.SparseCSFTensor` instance.

.. function:: bool arrow::py::is_sparse_csr_matrix(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :type:`SparseCSRMatrix` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.SparseCSRMatrix` instance.


The following functions expect a pyarrow object, unwrap the underlying
Arrow C++ API pointer, and return it as a :class:`Result` object.  An error
may be returned if the input object doesn't have the expected type.

.. function:: Result<std::shared_ptr<Array>> arrow::py::unwrap_array(PyObject* obj)

   Unwrap and return the Arrow C++ :class:`Array` pointer from *obj*.

.. function:: Result<std::shared_ptr<RecordBatch>> arrow::py::unwrap_batch(PyObject* obj)

   Unwrap and return the Arrow C++ :class:`RecordBatch` pointer from *obj*.

.. function:: Result<std::shared_ptr<Buffer>> arrow::py::unwrap_buffer(PyObject* obj)

   Unwrap and return the Arrow C++ :class:`Buffer` pointer from *obj*.

.. function:: Result<std::shared_ptr<DataType>> arrow::py::unwrap_data_type(PyObject* obj)

   Unwrap and return the Arrow C++ :class:`DataType` pointer from *obj*.

.. function:: Result<std::shared_ptr<Field>> arrow::py::unwrap_field(PyObject* obj)

   Unwrap and return the Arrow C++ :class:`Field` pointer from *obj*.

.. function:: Result<std::shared_ptr<Scalar>> arrow::py::unwrap_scalar(PyObject* obj)

   Unwrap and return the Arrow C++ :class:`Scalar` pointer from *obj*.

.. function:: Result<std::shared_ptr<Schema>> arrow::py::unwrap_schema(PyObject* obj)

   Unwrap and return the Arrow C++ :class:`Schema` pointer from *obj*.

.. function:: Result<std::shared_ptr<Table>> arrow::py::unwrap_table(PyObject* obj)

   Unwrap and return the Arrow C++ :class:`Table` pointer from *obj*.

.. function:: Result<std::shared_ptr<Tensor>> arrow::py::unwrap_tensor(PyObject* obj)

   Unwrap and return the Arrow C++ :class:`Tensor` pointer from *obj*.

.. function:: Result<std::shared_ptr<SparseCOOTensor>> arrow::py::unwrap_sparse_coo_tensor(PyObject* obj)

   Unwrap and return the Arrow C++ :type:`SparseCOOTensor` pointer from *obj*.

.. function:: Result<std::shared_ptr<SparseCSCMatrix>> arrow::py::unwrap_sparse_csc_matrix(PyObject* obj)

   Unwrap and return the Arrow C++ :type:`SparseCSCMatrix` pointer from *obj*.

.. function:: Result<std::shared_ptr<SparseCSFTensor>> arrow::py::unwrap_sparse_csf_tensor(PyObject* obj)

   Unwrap and return the Arrow C++ :type:`SparseCSFTensor` pointer from *obj*.

.. function:: Result<std::shared_ptr<SparseCSRMatrix>> arrow::py::unwrap_sparse_csr_matrix(PyObject* obj)

   Unwrap and return the Arrow C++ :type:`SparseCSRMatrix` pointer from *obj*.


The following functions take an Arrow C++ API pointer and wrap it in a
pyarray object of the corresponding type.  A new reference is returned.
On error, NULL is returned and a Python exception is set.

.. function:: PyObject* arrow::py::wrap_array(const std::shared_ptr<Array>& array)

   Wrap the Arrow C++ *array* in a :py:class:`pyarrow.Array` instance.

.. function:: PyObject* arrow::py::wrap_batch(const std::shared_ptr<RecordBatch>& batch)

   Wrap the Arrow C++ record *batch* in a :py:class:`pyarrow.RecordBatch` instance.

.. function:: PyObject* arrow::py::wrap_buffer(const std::shared_ptr<Buffer>& buffer)

   Wrap the Arrow C++ *buffer* in a :py:class:`pyarrow.Buffer` instance.

.. function:: PyObject* arrow::py::wrap_data_type(const std::shared_ptr<DataType>& data_type)

   Wrap the Arrow C++ *data_type* in a :py:class:`pyarrow.DataType` instance.

.. function:: PyObject* arrow::py::wrap_field(const std::shared_ptr<Field>& field)

   Wrap the Arrow C++ *field* in a :py:class:`pyarrow.Field` instance.

.. function:: PyObject* arrow::py::wrap_scalar(const std::shared_ptr<Scalar>& scalar)

   Wrap the Arrow C++ *scalar* in a :py:class:`pyarrow.Scalar` instance.

.. function:: PyObject* arrow::py::wrap_schema(const std::shared_ptr<Schema>& schema)

   Wrap the Arrow C++ *schema* in a :py:class:`pyarrow.Schema` instance.

.. function:: PyObject* arrow::py::wrap_table(const std::shared_ptr<Table>& table)

   Wrap the Arrow C++ *table* in a :py:class:`pyarrow.Table` instance.

.. function:: PyObject* arrow::py::wrap_tensor(const std::shared_ptr<Tensor>& tensor)

   Wrap the Arrow C++ *tensor* in a :py:class:`pyarrow.Tensor` instance.

.. function:: PyObject* arrow::py::wrap_sparse_coo_tensor(const std::shared_ptr<SparseCOOTensor>& sparse_tensor)

   Wrap the Arrow C++ *sparse_tensor* in a :py:class:`pyarrow.SparseCOOTensor` instance.

.. function:: PyObject* arrow::py::wrap_sparse_csc_matrix(const std::shared_ptr<SparseCSCMatrix>& sparse_tensor)

   Wrap the Arrow C++ *sparse_tensor* in a :py:class:`pyarrow.SparseCSCMatrix` instance.

.. function:: PyObject* arrow::py::wrap_sparse_csf_tensor(const std::shared_ptr<SparseCSFTensor>& sparse_tensor)

   Wrap the Arrow C++ *sparse_tensor* in a :py:class:`pyarrow.SparseCSFTensor` instance.

.. function:: PyObject* arrow::py::wrap_sparse_csr_matrix(const std::shared_ptr<SparseCSRMatrix>& sparse_tensor)

   Wrap the Arrow C++ *sparse_tensor* in a :py:class:`pyarrow.SparseCSRMatrix` instance.


Cython API
----------

.. default-domain:: py

The Cython API more or less mirrors the C++ API, but the calling convention
can be different as required by Cython.  In Cython, you don't need to
initialize the API as that will be handled automatically by the ``cimport``
directive.

.. note::
   Classes from the Arrow C++ API are renamed when exposed in Cython, to
   avoid named clashes with the corresponding Python classes.  For example,
   C++ Arrow arrays have the ``CArray`` type and ``Array`` is the
   corresponding Python wrapper class.

Wrapping and Unwrapping
~~~~~~~~~~~~~~~~~~~~~~~

The following functions expect a pyarrow object, unwrap the underlying
Arrow C++ API pointer, and return it.  NULL is returned (without setting
an exception) if the input is not of the right type.

.. function:: pyarrow_unwrap_array(obj) -> shared_ptr[CArray]

   Unwrap the Arrow C++ :cpp:class:`Array` pointer from *obj*.

.. function:: pyarrow_unwrap_batch(obj) -> shared_ptr[CRecordBatch]

   Unwrap the Arrow C++ :cpp:class:`RecordBatch` pointer from *obj*.

.. function:: pyarrow_unwrap_buffer(obj) -> shared_ptr[CBuffer]

   Unwrap the Arrow C++ :cpp:class:`Buffer` pointer from *obj*.

.. function:: pyarrow_unwrap_data_type(obj) -> shared_ptr[CDataType]

   Unwrap the Arrow C++ :cpp:class:`CDataType` pointer from *obj*.

.. function:: pyarrow_unwrap_field(obj) -> shared_ptr[CField]

   Unwrap the Arrow C++ :cpp:class:`Field` pointer from *obj*.

.. function:: pyarrow_unwrap_scalar(obj) -> shared_ptr[CScalar]

   Unwrap the Arrow C++ :cpp:class:`Scalar` pointer from *obj*.

.. function:: pyarrow_unwrap_schema(obj) -> shared_ptr[CSchema]

   Unwrap the Arrow C++ :cpp:class:`Schema` pointer from *obj*.

.. function:: pyarrow_unwrap_table(obj) -> shared_ptr[CTable]

   Unwrap the Arrow C++ :cpp:class:`Table` pointer from *obj*.

.. function:: pyarrow_unwrap_tensor(obj) -> shared_ptr[CTensor]

   Unwrap the Arrow C++ :cpp:class:`Tensor` pointer from *obj*.

.. function:: pyarrow_unwrap_sparse_coo_tensor(obj) -> shared_ptr[CSparseCOOTensor]

   Unwrap the Arrow C++ :cpp:type:`SparseCOOTensor` pointer from *obj*.

.. function:: pyarrow_unwrap_sparse_csc_matrix(obj) -> shared_ptr[CSparseCSCMatrix]

   Unwrap the Arrow C++ :cpp:type:`SparseCSCMatrix` pointer from *obj*.

.. function:: pyarrow_unwrap_sparse_csf_tensor(obj) -> shared_ptr[CSparseCSFTensor]

   Unwrap the Arrow C++ :cpp:type:`SparseCSFTensor` pointer from *obj*.

.. function:: pyarrow_unwrap_sparse_csr_matrix(obj) -> shared_ptr[CSparseCSRMatrix]

   Unwrap the Arrow C++ :cpp:type:`SparseCSRMatrix` pointer from *obj*.


The following functions take a Arrow C++ API pointer and wrap it in a
pyarray object of the corresponding type.  An exception is raised on error.

.. function:: pyarrow_wrap_array(const shared_ptr[CArray]& array) -> object

   Wrap the Arrow C++ *array* in a Python :class:`pyarrow.Array` instance.

.. function:: pyarrow_wrap_batch(const shared_ptr[CRecordBatch]& batch) -> object

   Wrap the Arrow C++ record *batch* in a Python :class:`pyarrow.RecordBatch` instance.

.. function:: pyarrow_wrap_buffer(const shared_ptr[CBuffer]& buffer) -> object

   Wrap the Arrow C++ *buffer* in a Python :class:`pyarrow.Buffer` instance.

.. function:: pyarrow_wrap_data_type(const shared_ptr[CDataType]& data_type) -> object

   Wrap the Arrow C++ *data_type* in a Python :class:`pyarrow.DataType` instance.

.. function:: pyarrow_wrap_field(const shared_ptr[CField]& field) -> object

   Wrap the Arrow C++ *field* in a Python :class:`pyarrow.Field` instance.

.. function:: pyarrow_wrap_resizable_buffer(const shared_ptr[CResizableBuffer]& buffer) -> object

   Wrap the Arrow C++ resizable *buffer* in a Python :class:`pyarrow.ResizableBuffer` instance.

.. function:: pyarrow_wrap_scalar(const shared_ptr[CScalar]& scalar) -> object

   Wrap the Arrow C++ *scalar* in a Python :class:`pyarrow.Scalar` instance.

.. function:: pyarrow_wrap_schema(const shared_ptr[CSchema]& schema) -> object

   Wrap the Arrow C++ *schema* in a Python :class:`pyarrow.Schema` instance.

.. function:: pyarrow_wrap_table(const shared_ptr[CTable]& table) -> object

   Wrap the Arrow C++ *table* in a Python :class:`pyarrow.Table` instance.

.. function:: pyarrow_wrap_tensor(const shared_ptr[CTensor]& tensor) -> object

   Wrap the Arrow C++ *tensor* in a Python :class:`pyarrow.Tensor` instance.

.. function:: pyarrow_wrap_sparse_coo_tensor(const shared_ptr[CSparseCOOTensor]& sparse_tensor) -> object

   Wrap the Arrow C++ *COO sparse tensor* in a Python :class:`pyarrow.SparseCOOTensor` instance.

.. function:: pyarrow_wrap_sparse_csc_matrix(const shared_ptr[CSparseCSCMatrix]& sparse_tensor) -> object

   Wrap the Arrow C++ *CSC sparse tensor* in a Python :class:`pyarrow.SparseCSCMatrix` instance.

.. function:: pyarrow_wrap_sparse_csf_tensor(const shared_ptr[CSparseCSFTensor]& sparse_tensor) -> object

   Wrap the Arrow C++ *COO sparse tensor* in a Python :class:`pyarrow.SparseCSFTensor` instance.

.. function:: pyarrow_wrap_sparse_csr_matrix(const shared_ptr[CSparseCSRMatrix]& sparse_tensor) -> object

   Wrap the Arrow C++ *CSR sparse tensor* in a Python :class:`pyarrow.SparseCSRMatrix` instance.


Example
~~~~~~~

The following Cython module shows how to unwrap a Python object and call
the underlying C++ object's API.

.. code-block:: python

   # distutils: language=c++

   from pyarrow.lib cimport *


   def get_array_length(obj):
       # Just an example function accessing both the pyarrow Cython API
       # and the Arrow C++ API
       cdef shared_ptr[CArray] arr = pyarrow_unwrap_array(obj)
       if arr.get() == NULL:
           raise TypeError("not an array")
       return arr.get().length()

To build this module, you will need a slightly customized ``setup.py`` file
(this is assuming the file above is named ``example.pyx``):

.. code-block:: python

    from setuptools import setup
    from Cython.Build import cythonize

    import os
    import numpy as np
    import pyarrow as pa


    ext_modules = cythonize("example.pyx")

    for ext in ext_modules:
        # The Numpy C headers are currently required
        ext.include_dirs.append(np.get_include())
        ext.include_dirs.append(pa.get_include())
        ext.libraries.extend(pa.get_libraries())
        ext.library_dirs.extend(pa.get_library_dirs())

        if os.name == 'posix':
            ext.extra_compile_args.append('-std=c++17')

    setup(ext_modules=ext_modules)


Compile the extension:

.. code-block:: bash

    python setup.py build_ext --inplace

Building Extensions against PyPI Wheels
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Python wheels have the Arrow C++ libraries bundled in the top level
``pyarrow/`` install directory. On Linux and macOS, these libraries have an ABI
tag like ``libarrow.so.17`` which means that linking with ``-larrow`` using the
linker path provided by ``pyarrow.get_library_dirs()`` will not work right out
of the box. To fix this, you must run ``pyarrow.create_library_symlinks()``
once as a user with write access to the directory where pyarrow is
installed. This function will attempt to create symlinks like
``pyarrow/libarrow.so``. For example:

.. code-block:: bash

   pip install pyarrow
   python -c "import pyarrow; pyarrow.create_library_symlinks()"

Toolchain Compatibility (Linux)
"""""""""""""""""""""""""""""""

The Python wheels for Linux are built using the
`PyPA manylinux images <https://quay.io/organization/pypa>`_ which use
the CentOS `devtoolset-9`. In addition to the other notes
above, if you are compiling C++ using these shared libraries, you will need
to make sure you use a compatible toolchain as well or you might see a
segfault during runtime.

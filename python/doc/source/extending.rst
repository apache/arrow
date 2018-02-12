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

.. currentmodule:: pyarrow.lib
.. _extending:

Using pyarrow from C++ and Cython Code
======================================

pyarrow features both a Cython and C++ API.

C++ API
-------

.. default-domain:: cpp

The Arrow C++ header files are bundled with a pyarrow installation.
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
appropriate linker flags. We have provided functions ``pyarrow.get_libraries``
and ``pyarrow.get_library_dirs`` which return a list of library names and
likely library install locations (if you installed pyarrow with pip or
conda). These must be included when declaring your C extensions with distutils
(see below).

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

.. function:: bool is_array(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Array` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Array` instance.

.. function:: bool is_buffer(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Buffer` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Buffer` instance.

.. function:: bool is_column(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Column` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Column` instance.

.. function:: bool is_data_type(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`DataType` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.DataType` instance.

.. function:: bool is_field(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Field` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Field` instance.

.. function:: bool is_record_batch(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`RecordBatch` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.RecordBatch` instance.

.. function:: bool is_schema(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Schema` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Schema` instance.

.. function:: bool is_table(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Table` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Table` instance.

.. function:: bool is_tensor(PyObject* obj)

   Return whether *obj* wraps an Arrow C++ :class:`Tensor` pointer;
   in other words, whether *obj* is a :py:class:`pyarrow.Tensor` instance.

The following functions expect a pyarrow object, unwrap the underlying
Arrow C++ API pointer, and put it in the *out* parameter.  The returned
:class:`Status` object must be inspected first to know whether any error
occurred.  If successful, *out* is guaranteed to be non-NULL.

.. function:: Status unwrap_array(PyObject* obj, std::shared_ptr<Array>* out)

   Unwrap the Arrow C++ :class:`Array` pointer from *obj* and put it in *out*.

.. function:: Status unwrap_buffer(PyObject* obj, std::shared_ptr<Buffer>* out)

   Unwrap the Arrow C++ :class:`Buffer` pointer from *obj* and put it in *out*.

.. function:: Status unwrap_column(PyObject* obj, std::shared_ptr<Column>* out)

   Unwrap the Arrow C++ :class:`Column` pointer from *obj* and put it in *out*.

.. function:: Status unwrap_data_type(PyObject* obj, std::shared_ptr<DataType>* out)

   Unwrap the Arrow C++ :class:`DataType` pointer from *obj* and put it in *out*.

.. function:: Status unwrap_field(PyObject* obj, std::shared_ptr<Field>* out)

   Unwrap the Arrow C++ :class:`Field` pointer from *obj* and put it in *out*.

.. function:: Status unwrap_record_batch(PyObject* obj, std::shared_ptr<RecordBatch>* out)

   Unwrap the Arrow C++ :class:`RecordBatch` pointer from *obj* and put it in *out*.

.. function:: Status unwrap_schema(PyObject* obj, std::shared_ptr<Schema>* out)

   Unwrap the Arrow C++ :class:`Schema` pointer from *obj* and put it in *out*.

.. function:: Status unwrap_table(PyObject* obj, std::shared_ptr<Table>* out)

   Unwrap the Arrow C++ :class:`Table` pointer from *obj* and put it in *out*.

.. function:: Status unwrap_tensor(PyObject* obj, std::shared_ptr<Tensor>* out)

   Unwrap the Arrow C++ :class:`Tensor` pointer from *obj* and put it in *out*.

The following functions take an Arrow C++ API pointer and wrap it in a
pyarray object of the corresponding type.  A new reference is returned.
On error, NULL is returned and a Python exception is set.

.. function:: PyObject* wrap_array(const std::shared_ptr<Array>& array)

   Wrap the Arrow C++ *array* in a :py:class:`pyarrow.Array` instance.

.. function:: PyObject* wrap_buffer(const std::shared_ptr<Buffer>& buffer)

   Wrap the Arrow C++ *buffer* in a :py:class:`pyarrow.Buffer` instance.

.. function:: PyObject* wrap_column(const std::shared_ptr<Column>& column)

   Wrap the Arrow C++ *column* in a :py:class:`pyarrow.Column` instance.

.. function:: PyObject* wrap_data_type(const std::shared_ptr<DataType>& data_type)

   Wrap the Arrow C++ *data_type* in a :py:class:`pyarrow.DataType` instance.

.. function:: PyObject* wrap_field(const std::shared_ptr<Field>& field)

   Wrap the Arrow C++ *field* in a :py:class:`pyarrow.Field` instance.

.. function:: PyObject* wrap_record_batch(const std::shared_ptr<RecordBatch>& batch)

   Wrap the Arrow C++ record *batch* in a :py:class:`pyarrow.RecordBatch` instance.

.. function:: PyObject* wrap_schema(const std::shared_ptr<Schema>& schema)

   Wrap the Arrow C++ *schema* in a :py:class:`pyarrow.Schema` instance.

.. function:: PyObject* wrap_table(const std::shared_ptr<Table>& table)

   Wrap the Arrow C++ *table* in a :py:class:`pyarrow.Table` instance.

.. function:: PyObject* wrap_tensor(const std::shared_ptr<Tensor>& tensor)

   Wrap the Arrow C++ *tensor* in a :py:class:`pyarrow.Tensor` instance.


Cython API
----------

.. default-domain:: py

The Cython API more or less mirrors the C++ API, but the calling convention
can be different as required by Cython.  In Cython, you don't need to
initialize the API as that will be handled automaticalled by the ``cimport``
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

.. function:: pyarrow_unwrap_column(obj) -> shared_ptr[CColumn]

   Unwrap the Arrow C++ :cpp:class:`Column` pointer from *obj*.

.. function:: pyarrow_unwrap_data_type(obj) -> shared_ptr[CDataType]

   Unwrap the Arrow C++ :cpp:class:`CDataType` pointer from *obj*.

.. function:: pyarrow_unwrap_field(obj) -> shared_ptr[CField]

   Unwrap the Arrow C++ :cpp:class:`Field` pointer from *obj*.

.. function:: pyarrow_unwrap_schema(obj) -> shared_ptr[CSchema]

   Unwrap the Arrow C++ :cpp:class:`Schema` pointer from *obj*.

.. function:: pyarrow_unwrap_table(obj) -> shared_ptr[CTable]

   Unwrap the Arrow C++ :cpp:class:`Table` pointer from *obj*.

.. function:: pyarrow_unwrap_tensor(obj) -> shared_ptr[CTensor]

   Unwrap the Arrow C++ :cpp:class:`Tensor` pointer from *obj*.

The following functions take a Arrow C++ API pointer and wrap it in a
pyarray object of the corresponding type.  An exception is raised on error.

.. function:: pyarrow_wrap_array(sp_array: const shared_ptr[CArray]& array) -> object

   Wrap the Arrow C++ *array* in a Python :class:`pyarrow.Array` instance.

.. function:: pyarrow_wrap_batch(sp_array: const shared_ptr[CRecordBatch]& batch) -> object

   Wrap the Arrow C++ record *batch* in a Python :class:`pyarrow.RecordBatch` instance.

.. function:: pyarrow_wrap_buffer(sp_array: const shared_ptr[CBuffer]& buffer) -> object

   Wrap the Arrow C++ *buffer* in a Python :class:`pyarrow.Buffer` instance.

.. function:: pyarrow_wrap_column(sp_array: const shared_ptr[CColumn]& column) -> object

   Wrap the Arrow C++ *column* in a Python :class:`pyarrow.Column` instance.

.. function:: pyarrow_wrap_data_type(sp_array: const shared_ptr[CDataType]& data_type) -> object

   Wrap the Arrow C++ *data_type* in a Python :class:`pyarrow.DataType` instance.

.. function:: pyarrow_wrap_field(sp_array: const shared_ptr[CField]& field) -> object

   Wrap the Arrow C++ *field* in a Python :class:`pyarrow.Field` instance.

.. function:: pyarrow_wrap_resizable_buffer(sp_array: const shared_ptr[CResizableBuffer]& buffer) -> object

   Wrap the Arrow C++ resizable *buffer* in a Python :class:`pyarrow.ResizableBuffer` instance.

.. function:: pyarrow_wrap_schema(sp_array: const shared_ptr[CSchema]& schema) -> object

   Wrap the Arrow C++ *schema* in a Python :class:`pyarrow.Schema` instance.

.. function:: pyarrow_wrap_table(sp_array: const shared_ptr[CTable]& table) -> object

   Wrap the Arrow C++ *table* in a Python :class:`pyarrow.Table` instance.

.. function:: pyarrow_wrap_tensor(sp_array: const shared_ptr[CTensor]& tensor) -> object

   Wrap the Arrow C++ *tensor* in a Python :class:`pyarrow.Tensor` instance.

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

    from distutils.core import setup
    from Cython.Build import cythonize

    import numpy as np

    import pyarrow as pa

    ext_modules = cythonize("example.pyx")

    for ext in ext_modules:
        # The Numpy C headers are currently required
        ext.include_dirs.append(np.get_include())
        ext.include_dirs.append(pa.get_include())
        ext.libraries.extend(pa.get_libraries())
        ext.library_dirs.append(pa.get_library_dirs())

    setup(
        ext_modules=ext_modules,
    )

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


=============================
The Arrow PyCapsule Interface
=============================

.. warning:: The Arrow PyCapsule Interface should be considered experimental

Rationale
=========

The :ref:`C data interface <c-data-interface>` and
:ref:`C stream interface <c-stream-interface>` allow moving Arrow data between
different implementations of Arrow. However, these interfaces don't specify how
Python libraries should expose these structs to other libraries. Prior to this,
many libraries simply provided export to PyArrow data structures, using the
``_import_from_c`` and ``_export_to_c`` methods. However, this always required
PyArrow to be installed. In addition, those APIs could cause memory leaks if
handled improperly.

This interface allows any library to export Arrow data structures to other
libraries that understand the same protocol.

Goals
-----

* Standardize the `PyCapsule`_ objects that represent ``ArrowSchema``, ``ArrowArray``,
  and ``ArrowArrayStream``.
* Define standard methods that export Arrow data into such capsule objects,
  so that any Python library wanting to accept Arrow data as input can call the
  corresponding method instead of hardcoding support for specific Arrow
  producers.


Non-goals
---------

* Standardize what public APIs should be used for import. This is left up to
  individual libraries.

PyCapsule Standard
==================

When exporting Arrow data through Python, the C Data Interface / C Stream Interface
structures should be wrapped in capsules. Capsules avoid invalid access by
attaching a name to the pointer and avoid memory leaks by attaching a destructor.
Thus, they are much safer than passing pointers as integers.

`PyCapsule`_ allows for a ``name`` to be associated with the capsule, allowing 
consumers to verify that the capsule contains the expected kind of data. To make sure
Arrow structures are recognized, the following names must be used:

.. list-table::
   :widths: 25 25
   :header-rows: 1

   * - C Interface Type
     - PyCapsule Name
   * - ArrowSchema
     - ``arrow_schema``
   * - ArrowArray
     - ``arrow_array``
   * - ArrowArrayStream
     - ``arrow_array_stream``


Lifetime Semantics
------------------

The exported PyCapsules should have a destructor that calls the
:ref:`release callback <c-data-interface-released>`
of the Arrow struct, if it is not already null. This prevents a memory leak in
case the capsule was never passed to another consumer.

If the capsule has been passed to a consumer, the consumer should have moved
the data and marked the release callback as null, so there isn’t a risk of
releasing data the consumer is using.
:ref:`Read more in the C Data Interface specification <c-data-interface-released>`.

Just like in the C Data Interface, the PyCapsule objects defined here can only
be consumed once.

For an example of a PyCapsule with a destructor, see `Create a PyCapsule`_.


Export Protocol
===============

The interface consists of three separate protocols:

* ``ArrowSchemaExportable``, which defines the ``__arrow_c_schema__`` method.
* ``ArrowArrayExportable``, which defines the ``__arrow_c_array__`` method.
* ``ArrowStreamExportable``, which defines the ``__arrow_c_stream__`` method.

ArrowSchema Export
------------------

Schemas, fields, and data types can implement the method ``__arrow_c_schema__``.

.. py:method:: __arrow_c_schema__(self) -> object

    Export the object as an ArrowSchema.

    :return: A PyCapsule containing a C ArrowSchema representation of the
        object. The capsule must have a name of ``"arrow_schema"``.


ArrowArray Export
-----------------

Arrays and record batches (contiguous tables) can implement the method
``__arrow_c_array__``.

.. py:method:: __arrow_c_array__(self, requested_schema: object | None = None) -> Tuple[object, object]

    Export the object as a pair of ArrowSchema and ArrowArray structures.

    :param requested_schema: A PyCapsule containing a C ArrowSchema representation 
        of a requested schema. Conversion to this schema is best-effort. See 
        `Schema Requests`_.
    :type requested_schema: PyCapsule or None

    :return: A pair of PyCapsules containing a C ArrowSchema and ArrowArray,
        respectively. The schema capsule should have the name ``"arrow_schema"``
        and the array capsule should have the name ``"arrow_array"``.


ArrowStream Export
------------------

Tables / DataFrames and streams can implement the method ``__arrow_c_stream__``.

.. py:method:: __arrow_c_stream__(self, requested_schema: object | None = None) -> object

    Export the object as an ArrowArrayStream.

    :param requested_schema: A PyCapsule containing a C ArrowSchema representation 
        of a requested schema. Conversion to this schema is best-effort. See 
        `Schema Requests`_.
    :type requested_schema: PyCapsule or None

    :return: A PyCapsule containing a C ArrowArrayStream representation of the
        object. The capsule must have a name of ``"arrow_array_stream"``.

Schema Requests
---------------

In some cases, there might be multiple possible Arrow representations of the
same data. For example, a library might have a single integer type, but Arrow
has multiple integer types with different sizes and sign. As another example,
Arrow has several possible encodings for an array of strings: 32-bit offsets,
64-bit offsets, string view, and dictionary-encoded. A sequence of strings could
export to any one of these Arrow representations.

In order to allow the caller to request a specific representation, the
:meth:`__arrow_c_array__` and :meth:`__arrow_c_stream__` methods take an optional
``requested_schema`` parameter. This parameter is a PyCapsule containing an
``ArrowSchema``.

The callee should attempt to provide the data in the requested schema. However,
if the callee cannot provide the data in the requested schema, they may return
with the same schema as if ``None`` were passed to ``requested_schema``.

If the caller requests a schema that is not compatible with the data,
say requesting a schema with a different number of fields, the callee should
raise an exception. The requested schema mechanism is only meant to negotiate
between different representations of the same data and not to allow arbitrary
schema transformations.


.. _PyCapsule: https://docs.python.org/3/c-api/capsule.html


Protocol Typehints
------------------

The following typehints can be copied into your library to annotate that a 
function accepts an object implementing one of these protocols.

.. code-block:: python

    from typing import Tuple, Protocol
    from typing_extensions import Self

    class ArrowSchemaExportable(Protocol):
        def __arrow_c_schema__(self) -> object: ...

    class ArrowArrayExportable(Protocol):
        def __arrow_c_array__(
            self,
            requested_schema: object | None = None
        ) -> Tuple[object, object]:
            ...

    class ArrowStreamExportable(Protocol):
        def __arrow_c_stream__(
            self,
            requested_schema: object | None = None
        ) -> object:
            ...

Examples
========

Create a PyCapsule
------------------


To create a PyCapsule, use the `PyCapsule_New <https://docs.python.org/3/c-api/capsule.html#c.PyCapsule_New>`_
function. The function must be passed a destructor function that will be called
to release the data the capsule points to. It must first call the release
callback if it is not null, then free the struct.

Below is the code to create a PyCapsule for an ``ArrowSchema``. The code for
``ArrowArray`` and ``ArrowArrayStream`` is similar.

.. tab-set::

   .. tab-item:: C

      .. code-block:: c

         #include <Python.h>

         void ReleaseArrowSchemaPyCapsule(PyObject* capsule) {
             struct ArrowSchema* schema =
                 (struct ArrowSchema*)PyCapsule_GetPointer(capsule, "arrow_schema");
             if (schema->release != NULL) {
                 schema->release(schema);
             }
             free(schema);
         }
         
         PyObject* ExportArrowSchemaPyCapsule() {
             struct ArrowSchema* schema =
                 (struct ArrowSchema*)malloc(sizeof(struct ArrowSchema));
             // Fill in ArrowSchema fields
             // ...
             return PyCapsule_New(schema, "arrow_schema", ReleaseArrowSchemaPyCapsule);
         }

   .. tab-item:: Cython

      .. code-block:: cython

         cimport cpython
         from libc.stdlib cimport malloc, free

         cdef void release_arrow_schema_py_capsule(object schema_capsule):
             cdef ArrowSchema* schema = <ArrowSchema*>cpython.PyCapsule_GetPointer(
                 schema_capsule, 'arrow_schema'
             )
             if schema.release != NULL:
                 schema.release(schema)
         
             free(schema)
         
         cdef object export_arrow_schema_py_capsule():
             cdef ArrowSchema* schema = <ArrowSchema*>malloc(sizeof(ArrowSchema))
             # It's recommended to immediately wrap the struct in a capsule, so
             # if subsequent lines raise an exception memory will not be leaked.
             schema.release = NULL
             capsule = cpython.PyCapsule_New(
                 <void*>schema, 'arrow_schema', release_arrow_schema_py_capsule
             )
             # Fill in ArrowSchema fields:
             # schema.format = ...
             # ...
             return capsule


Consume a PyCapsule
-------------------

To consume a PyCapsule, use the `PyCapsule_GetPointer <https://docs.python.org/3/c-api/capsule.html#c.PyCapsule_GetPointer>`_ function
to get the pointer to the underlying struct. Import the struct using your
system's Arrow C Data Interface import function. Only after that should the
capsule be freed.

The below example shows how to consume a PyCapsule for an ``ArrowSchema``. The
code for ``ArrowArray`` and ``ArrowArrayStream`` is similar.

.. tab-set::

   .. tab-item:: C

      .. code-block:: c

         #include <Python.h>
         
         // If the capsule is not an ArrowSchema, will return NULL and set an exception.
         struct ArrowSchema* GetArrowSchemaPyCapsule(PyObject* capsule) {
           return PyCapsule_GetPointer(capsule, "arrow_schema");
         }

   .. tab-item:: Cython

      .. code-block:: cython

         cimport cpython
        
         cdef ArrowSchema* get_arrow_schema_py_capsule(object capsule) except NULL:
             return <ArrowSchema*>cpython.PyCapsule_GetPointer(capsule, 'arrow_schema')

Backwards Compatibility with PyArrow
------------------------------------

When interacting with PyArrow, the PyCapsule interface should be preferred over
the ``_export_to_c`` and ``_import_from_c`` methods. However, many libraries will
want to support a range of PyArrow versions. This can be done via Duck typing.

For example, if your library had an import method such as:

.. code-block:: python

   # OLD METHOD
   def from_arrow(arr: pa.Array)
       array_import_ptr = make_array_import_ptr()
       schema_import_ptr = make_schema_import_ptr()
       arr._export_to_c(array_import_ptr, schema_import_ptr)
       return import_c_data(array_import_ptr, schema_import_ptr)

You can rewrite this method to support both PyArrow and other libraries that
implement the PyCapsule interface:

.. code-block:: python

   # NEW METHOD
   def from_arrow(arr)
       # Newer versions of PyArrow as well as other libraries with Arrow data
       # implement this method, so prefer it over _export_to_c.
       if hasattr(arr, "__arrow_c_array__"):
            schema_ptr, array_ptr = arr.__arrow_c_array__()
            return import_c_capsule_data(schema_ptr, array_ptr)
       elif isinstance(arr, pa.Array):
            # Deprecated method, used for older versions of PyArrow
            array_import_ptr = make_array_import_ptr()
            schema_import_ptr = make_schema_import_ptr()
            arr._export_to_c(array_import_ptr, schema_import_ptr)
            return import_c_data(array_import_ptr, schema_import_ptr)
       else:
           raise TypeError(f"Cannot import {type(arr)} as Arrow array data.")

You may also wish to accept objects implementing the protocol in your
constructors. For example, in PyArrow, the :func:`array` and :func:`record_batch`
constructors accept any object that implements the :meth:`__arrow_c_array__` method
protocol. Similarly, the PyArrow's :func:`schema` constructor accepts any object
that implements the :meth:`__arrow_c_schema__` method.

Now if your library has an export to PyArrow function, such as:

.. code-block:: python

   # OLD METHOD
   def to_arrow(self) -> pa.Array:
       array_export_ptr = make_array_export_ptr()
       schema_export_ptr = make_schema_export_ptr()
       self.export_c_data(array_export_ptr, schema_export_ptr)
       return pa.Array._import_from_c(array_export_ptr, schema_export_ptr)

You can rewrite this function to use the PyCapsule interface by passing your
object to the :py:func:`array` constructor, which accepts any object that
implements the protocol. An easy way to check if the PyArrow version is new
enough to support this is to check whether ``pa.Array`` has the
``__arrow_c_array__`` method.

.. code-block:: python

  import warnings

  # NEW METHOD
  def to_arrow(self) -> pa.Array:
      # PyArrow added support for constructing arrays from objects implementing
      # __arrow_c_array__ in the same version it added the method for it's own
      # arrays. So we can use hasattr to check if the method is available as
      # a proxy for checking the PyArrow version.
      if hasattr(pa.Array, "__arrow_c_array__"):
          return pa.array(self)
      else:
          array_export_ptr = make_array_export_ptr()
          schema_export_ptr = make_schema_export_ptr()
          self.export_c_data(array_export_ptr, schema_export_ptr)
          return pa.Array._import_from_c(array_export_ptr, schema_export_ptr)


Comparison with Other Protocols
===============================

Comparison to DataFrame Interchange Protocol
--------------------------------------------

`The DataFrame Interchange Protocol <https://data-apis.org/dataframe-protocol/latest/>`_
is another protocol in Python that allows for the sharing of data between libraries.
This protocol is complementary to the DataFrame Interchange Protocol. Many of
the objects that implement this protocol will also implement the DataFrame
Interchange Protocol.

This protocol is specific to Arrow-based data structures, while the DataFrame
Interchange Protocol allows non-Arrow data frames and arrays to be shared as well.
Because of this, these PyCapsules can support Arrow-specific features such as
nested columns.

This protocol is also much more minimal than the DataFrame Interchange Protocol.
It just handles data export, rather than defining accessors for details like
number of rows or columns.

In summary, if you are implementing this protocol, you should also consider
implementing the DataFrame Interchange Protocol.


Comparison to ``__arrow_array__`` protocol
------------------------------------------

The :ref:`arrow_array_protocol` protocol is a dunder method that 
defines how PyArrow should import an object as an Arrow array. Unlike this
protocol, it is specific to PyArrow and isn't used by other libraries. It is
also limited to arrays and does not support schemas, tabular structures, or streams.
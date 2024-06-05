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
.. _extending_types:

Extending pyarrow
=================

Controlling conversion to (Py)Arrow with the PyCapsule Interface
----------------------------------------------------------------

The :ref:`Arrow C data interface <c-data-interface>` allows moving Arrow data between
different implementations of Arrow. This is a generic, cross-language interface not
specific to Python, but for Python libraries this interface is extended with a Python
specific layer: :ref:`arrow-pycapsule-interface`.

This Python interface ensures that different libraries that support the C Data interface
can export Arrow data structures in a standard way and recognize each other's objects.

If you have a Python library providing data structures that hold Arrow-compatible data
under the hood, you can implement the following methods on those objects:

- ``__arrow_c_schema__`` for schema or type-like objects.
- ``__arrow_c_array__`` for arrays and record batches (contiguous tables).
- ``__arrow_c_stream__`` for chunked arrays, tables and streams of data.

Those methods return `PyCapsule <https://docs.python.org/3/c-api/capsule.html>`__
objects, and more details on the exact semantics can be found in the
:ref:`specification <arrow-pycapsule-interface>`.

When your data structures have those methods defined, the PyArrow constructors
(see below) will recognize those objects as
supporting this protocol, and convert them to PyArrow data structures zero-copy. And the
same can be true for any other library supporting this protocol on ingesting data.

Similarly, if your library has functions that accept user-provided data, you can add
support for this protocol by checking for the presence of those methods, and
therefore accept any Arrow data (instead of harcoding support for a specific
Arrow producer such as PyArrow).

For consuming data through this protocol with PyArrow, the following constructors
can be used to create the various PyArrow objects:

+----------------------------+-----------------------------------------------+--------------------+
| Result class               | PyArrow constructor                           | Supported protocol |
+============================+===============================================+====================+
| :class:`Array`             | :func:`pyarrow.array`                         | array              |
+----------------------------+-----------------------------------------------+--------------------+
| :class:`ChunkedArray`      | :func:`pyarrow.chunked_array`                 | array, stream      |
+----------------------------+-----------------------------------------------+--------------------+
| :class:`RecordBatch`       | :func:`pyarrow.record_batch`                  | array              |
+----------------------------+-----------------------------------------------+--------------------+
| :class:`Table`             | :func:`pyarrow.table`                         | array, stream      |
+----------------------------+-----------------------------------------------+--------------------+
| :class:`RecordBatchReader` | :meth:`pyarrow.RecordBatchReader.from_stream` | stream             |
+----------------------------+-----------------------------------------------+--------------------+
| :class:`Field`             | :func:`pyarrow.field`                         | schema             |
+----------------------------+-----------------------------------------------+--------------------+
| :class:`Schema`            | :func:`pyarrow.schema`                        | schema             |
+----------------------------+-----------------------------------------------+--------------------+

A :class:`DataType` can be created by consuming the schema-compatible object
using :func:`pyarrow.field` and then accessing the ``.type`` of the resulting
Field.

.. _arrow_array_protocol:

Controlling conversion to pyarrow.Array with the ``__arrow_array__`` protocol
-----------------------------------------------------------------------------

The :func:`pyarrow.array` function has built-in support for Python sequences,
numpy arrays and pandas 1D objects (Series, Index, Categorical, ..) to convert
those to Arrow arrays. This can be extended for other array-like objects
by implementing the ``__arrow_array__`` method (similar to numpy's ``__array__``
protocol).

For example, to support conversion of your duck array class to an Arrow array,
define the ``__arrow_array__`` method to return an Arrow array::

    class MyDuckArray:

        ...

        def __arrow_array__(self, type=None):
            # convert the underlying array values to a pyarrow Array
            import pyarrow
            return pyarrow.array(..., type=type)

The ``__arrow_array__`` method takes an optional ``type`` keyword which is passed
through from :func:`pyarrow.array`. The method is allowed to return either
a :class:`~pyarrow.Array` or a :class:`~pyarrow.ChunkedArray`.

.. note::

    For a more general way to control the conversion of Python objects to Arrow
    data consider the :doc:`/format/CDataInterface/PyCapsuleInterface`. It is
    not specific to PyArrow and supports converting other objects such as tables
    and schemas.


Defining extension types ("user-defined types")
-----------------------------------------------

Arrow has the notion of extension types in the metadata specification as a
possibility to extend the built-in types. This is done by annotating any of the
built-in Arrow logical types (the "storage type") with a custom type name and
optional serialized representation ("ARROW:extension:name" and
"ARROW:extension:metadata" keys in the Field’s custom_metadata of an IPC
message).
See the :ref:`format_metadata_extension_types` section of the metadata
specification for more details.

Pyarrow allows you to define such extension types from Python by subclassing
:class:`ExtensionType` and giving the derived class its own extension name
and serialization mechanism. The extension name and serialized metadata
can potentially be recognized by other (non-Python) Arrow implementations
such as PySpark.

For example, we could define a custom UUID type for 128-bit numbers which can
be represented as ``FixedSizeBinary`` type with 16 bytes::

    class UuidType(pa.ExtensionType):

        def __init__(self):
            super().__init__(pa.binary(16), "my_package.uuid")

        def __arrow_ext_serialize__(self):
            # Since we don't have a parameterized type, we don't need extra
            # metadata to be deserialized
            return b''

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            # Sanity checks, not required but illustrate the method signature.
            assert storage_type == pa.binary(16)
            assert serialized == b''
            # Return an instance of this subclass given the serialized
            # metadata.
            return UuidType()

The special methods ``__arrow_ext_serialize__`` and ``__arrow_ext_deserialize__``
define the serialization of an extension type instance. For non-parametric
types such as the above, the serialization payload can be left empty.

This can now be used to create arrays and tables holding the extension type::

    >>> uuid_type = UuidType()
    >>> uuid_type.extension_name
    'my_package.uuid'
    >>> uuid_type.storage_type
    FixedSizeBinaryType(fixed_size_binary[16])

    >>> import uuid
    >>> storage_array = pa.array([uuid.uuid4().bytes for _ in range(4)], pa.binary(16))
    >>> arr = pa.ExtensionArray.from_storage(uuid_type, storage_array)
    >>> arr
    <pyarrow.lib.ExtensionArray object at 0x7f75c2f300a0>
    [
      A6861959108644B797664AEEE686B682,
      718747F48E5F4058A7261E2B6B228BE8,
      7FE201227D624D96A5CD8639DEF2A68B,
      C6CA8C7F95744BFD9462A40B3F57A86C
    ]

This array can be included in RecordBatches, sent over IPC and received in
another Python process. The receiving process must explicitly register the
extension type for deserialization, otherwise it will fall back to the
storage type::

    >>> pa.register_extension_type(UuidType())

For example, creating a RecordBatch and writing it to a stream using the
IPC protocol::

    >>> batch = pa.RecordBatch.from_arrays([arr], ["ext"])
    >>> sink = pa.BufferOutputStream()
    >>> with pa.RecordBatchStreamWriter(sink, batch.schema) as writer:
    ...    writer.write_batch(batch)
    >>> buf = sink.getvalue()

and then reading it back yields the proper type::

    >>> with pa.ipc.open_stream(buf) as reader:
    ...    result = reader.read_all()
    >>> result.column('ext').type
    UuidType(FixedSizeBinaryType(fixed_size_binary[16]))

The receiving application doesn't need to be Python but can still recognize
the extension type as a "my_package.uuid" type, if it has implemented its own
extension type to receive it. If the type is not registered in the receiving
application, it will fall back to the storage type.

Parameterized extension type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The above example used a fixed storage type with no further metadata. But
more flexible, parameterized extension types are also possible.

The example given here implements an extension type for the `pandas "period"
data type <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#time-span-representation>`__,
representing time spans (e.g., a frequency of a day, a month, a quarter, etc).
It is stored as an int64 array which is interpreted as the number of time spans
of the given frequency since 1970.

::

    class PeriodType(pa.ExtensionType):

        def __init__(self, freq):
            # attributes need to be set first before calling
            # super init (as that calls serialize)
            self._freq = freq
            super().__init__(pa.int64(), 'my_package.period')

        @property
        def freq(self):
            return self._freq

        def __arrow_ext_serialize__(self):
            return "freq={}".format(self.freq).encode()

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            # Return an instance of this subclass given the serialized
            # metadata.
            serialized = serialized.decode()
            assert serialized.startswith("freq=")
            freq = serialized.split('=')[1]
            return PeriodType(freq)

Here, we ensure to store all information in the serialized metadata that is
needed to reconstruct the instance (in the ``__arrow_ext_deserialize__`` class
method), in this case the frequency string.

Note that, once created, the data type instance is considered immutable.
In the example above, the ``freq`` parameter is therefore stored in a private
attribute with a public read-only property to access it.

Custom extension array class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, all arrays with an extension type are constructed or deserialized into
a built-in :class:`ExtensionArray` object. Nevertheless, one could want to subclass
:class:`ExtensionArray` in order to add some custom logic specific to the extension
type. Arrow allows to do so by adding a special method ``__arrow_ext_class__`` to the
definition of the extension type.

For instance, let us consider the example from the `Numpy Quickstart <https://docs.scipy.org/doc/numpy-1.13.0/user/quickstart.html>`_ of points in 3D space.
We can store these as a fixed-size list, where we wish to be able to extract
the data as a 2-D Numpy array ``(N, 3)`` without any copy::

    class Point3DArray(pa.ExtensionArray):
        def to_numpy_array(self):
            return self.storage.flatten().to_numpy().reshape((-1, 3))


    class Point3DType(pa.ExtensionType):
        def __init__(self):
            super().__init__(pa.list_(pa.float32(), 3), "my_package.Point3DType")

        def __arrow_ext_serialize__(self):
            return b''

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            return Point3DType()

        def __arrow_ext_class__(self):
            return Point3DArray

Arrays built using this extension type now have the expected custom array class::

    >>> storage = pa.array([[1, 2, 3], [4, 5, 6]], pa.list_(pa.float32(), 3))
    >>> arr = pa.ExtensionArray.from_storage(Point3DType(), storage)
    >>> arr
    <__main__.Point3DArray object at 0x7f40dea80670>
    [
        [
            1,
            2,
            3
        ],
        [
            4,
            5,
            6
        ]
    ]

The additional methods in the extension class are then available to the user::

    >>> arr.to_numpy_array()
    array([[1., 2., 3.],
       [4., 5., 6.]], dtype=float32)


This array can be sent over IPC, received in another Python process, and the custom
extension array class will be preserved (as long as the receiving process registers
the extension type using :func:`register_extension_type` before reading the IPC data).

Custom scalar conversion
~~~~~~~~~~~~~~~~~~~~~~~~

If you want scalars of your custom extension type to convert to a custom type when
:meth:`ExtensionScalar.as_py()` is called, you can override the
:meth:`ExtensionScalar.as_py()` method by subclassing :class:`ExtensionScalar`.
For example, if we wanted the above example 3D point type to return a custom
3D point class instead of a list, we would implement::

    from collections import namedtuple

    Point3D = namedtuple("Point3D", ["x", "y", "z"])

    class Point3DScalar(pa.ExtensionScalar):
        def as_py(self) -> Point3D:
            return Point3D(*self.value.as_py())

    class Point3DType(pa.ExtensionType):
        def __init__(self):
            super().__init__(pa.list_(pa.float32(), 3), "my_package.Point3DType")

        def __arrow_ext_serialize__(self):
            return b''

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            return Point3DType()

        def __arrow_ext_scalar_class__(self):
            return Point3DScalar

Arrays built using this extension type now provide scalars that convert to our ``Point3D`` class::

    >>> storage = pa.array([[1, 2, 3], [4, 5, 6]], pa.list_(pa.float32(), 3))
    >>> arr = pa.ExtensionArray.from_storage(Point3DType(), storage)
    >>> arr[0].as_py()
    Point3D(x=1.0, y=2.0, z=3.0)

    >>> arr.to_pylist()
    [Point3D(x=1.0, y=2.0, z=3.0), Point3D(x=4.0, y=5.0, z=6.0)]


Conversion to pandas
~~~~~~~~~~~~~~~~~~~~

The conversion to pandas (in :meth:`Table.to_pandas`) of columns with an
extension type can controlled in case there is a corresponding
`pandas extension array <https://pandas.pydata.org/pandas-docs/stable/development/extending.html#extension-types>`__
for your extension type.

For this, the :meth:`ExtensionType.to_pandas_dtype` method needs to be
implemented, and should return a ``pandas.api.extensions.ExtensionDtype``
subclass instance.

Using the pandas period type from above as example, this would look like::

    class PeriodType(pa.ExtensionType):
        ...

        def to_pandas_dtype(self):
            import pandas as pd
            return pd.PeriodDtype(freq=self.freq)

Secondly, the pandas ``ExtensionDtype`` on its turn needs to have the
``__from_arrow__`` method implemented: a method that given a pyarrow Array
or ChunkedArray of the extension type can construct the corresponding
pandas ``ExtensionArray``. This method should have the following signature::


    class MyExtensionDtype(pd.api.extensions.ExtensionDtype):
        ...

        def __from_arrow__(self, array: pyarrow.Array/ChunkedArray) -> pandas.ExtensionArray:
            ...

This way, you can control the conversion of a pyarrow ``Array`` of your pyarrow
extension type to a pandas ``ExtensionArray`` that can be stored in a DataFrame.


Canonical extension types
~~~~~~~~~~~~~~~~~~~~~~~~~

You can find the official list of canonical extension types in the
:ref:`format_canonical_extensions` section. Here we add examples on how to
use them in pyarrow.

Fixed size tensor
"""""""""""""""""

To create an array of tensors with equal shape (fixed shape tensor array) we
first need to define a fixed shape tensor extension type with value type
and shape:

.. code-block:: python

   >>> tensor_type = pa.fixed_shape_tensor(pa.int32(), (2, 2))

Then we need the storage array with :func:`pyarrow.list_` type where ``value_type```
is the fixed shape tensor value type and list size is a product of ``tensor_type``
shape elements. Then we can create an array of tensors with
``pa.ExtensionArray.from_storage()`` method:

.. code-block:: python

   >>> arr = [[1, 2, 3, 4], [10, 20, 30, 40], [100, 200, 300, 400]]
   >>> storage = pa.array(arr, pa.list_(pa.int32(), 4))
   >>> tensor_array = pa.ExtensionArray.from_storage(tensor_type, storage)

We can also create another array of tensors with different value type:

.. code-block:: python

   >>> tensor_type_2 = pa.fixed_shape_tensor(pa.float32(), (2, 2))
   >>> storage_2 = pa.array(arr, pa.list_(pa.float32(), 4))
   >>> tensor_array_2 = pa.ExtensionArray.from_storage(tensor_type_2, storage_2)

Extension arrays can be used as columns in  ``pyarrow.Table`` or
``pyarrow.RecordBatch``:

.. code-block:: python

   >>> data = [
   ...     pa.array([1, 2, 3]),
   ...     pa.array(['foo', 'bar', None]),
   ...     pa.array([True, None, True]),
   ...     tensor_array,
   ...     tensor_array_2
   ... ]
   >>> my_schema = pa.schema([('f0', pa.int8()),
   ...                        ('f1', pa.string()),
   ...                        ('f2', pa.bool_()),
   ...                        ('tensors_int', tensor_type),
   ...                        ('tensors_float', tensor_type_2)])
   >>> table = pa.Table.from_arrays(data, schema=my_schema)
   >>> table
   pyarrow.Table
   f0: int8
   f1: string
   f2: bool
   tensors_int: extension<arrow.fixed_shape_tensor[value_type=int32, shape=[2,2]]>
   tensors_float: extension<arrow.fixed_shape_tensor[value_type=float, shape=[2,2]]>
   ----
   f0: [[1,2,3]]
   f1: [["foo","bar",null]]
   f2: [[true,null,true]]
   tensors_int: [[[1,2,3,4],[10,20,30,40],[100,200,300,400]]]
   tensors_float: [[[1,2,3,4],[10,20,30,40],[100,200,300,400]]]

We can also convert a tensor array to a single multi-dimensional numpy ndarray.
With the conversion the length of the arrow array becomes the first dimension
in the numpy ndarray:

.. code-block:: python

   >>> numpy_tensor = tensor_array_2.to_numpy_ndarray()
   >>> numpy_tensor
   array([[[  1.,   2.],
           [  3.,   4.]],
          [[ 10.,  20.],
           [ 30.,  40.]],
          [[100., 200.],
           [300., 400.]]])
    >>> numpy_tensor.shape
   (3, 2, 2)

.. note::

   Both optional parameters, ``permutation`` and ``dim_names``, are meant to provide the user
   with the information about the logical layout of the data compared to the physical layout.

   The conversion to numpy ndarray is only possible for trivial permutations (``None`` or
   ``[0, 1, ... N-1]`` where ``N`` is the number of tensor dimensions).

And also the other way around, we can convert a numpy ndarray to a fixed shape tensor array:

.. code-block:: python

   >>> pa.FixedShapeTensorArray.from_numpy_ndarray(numpy_tensor)
   <pyarrow.lib.FixedShapeTensorArray object at ...>
   [
     [
       1,
       2,
       3,
       4
     ],
     [
       10,
       20,
       30,
       40
     ],
     [
       100,
       200,
       300,
       400
     ]
   ]

With the conversion the first dimension of the ndarray becomes the length of the pyarrow extension
array. We can see in the example that ndarray of shape ``(3, 2, 2)`` becomes an arrow array of
length 3 with tensor elements of shape ``(2, 2)``.

.. code-block:: python

   # ndarray of shape (3, 2, 2)
   >>> numpy_tensor.shape
   (3, 2, 2)

   # arrow array of length 3 with tensor elements of shape (2, 2)
   >>> pyarrow_tensor_array = pa.FixedShapeTensorArray.from_numpy_ndarray(numpy_tensor)
   >>> len(pyarrow_tensor_array)
   3
   >>> pyarrow_tensor_array.type.shape
   [2, 2]

The extension type can also have ``permutation`` and ``dim_names`` defined. For
example

.. code-block:: python

    >>> tensor_type = pa.fixed_shape_tensor(pa.float64(), [2, 2, 3], permutation=[0, 2, 1])

or

.. code-block:: python

    >>> tensor_type = pa.fixed_shape_tensor(pa.bool_(), [2, 2, 3], dim_names=['C', 'H', 'W'])

for ``NCHW`` format where:

* N: number of images which is in our case the length of an array and is always on
  the first dimension
* C: number of channels of the image
* H: height of the image
* W: width of the image

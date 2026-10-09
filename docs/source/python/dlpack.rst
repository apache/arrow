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

.. _pyarrow-dlpack:

The DLPack Protocol
===================

`The DLPack Protocol <https://github.com/dmlc/dlpack>`_
is a stable in-memory data structure that allows exchange
between major frameworks working with multidimensional
arrays or tensors. It is designed for cross hardware
support meaning it allows exchange of data on devices other
than the CPU (e.g. GPU).

DLPack protocol had been
`selected as the Python array API standard <https://data-apis.org/array-api/latest/design_topics/data_interchange.html#dlpack-an-in-memory-tensor-structure>`_
by the
`Consortium for Python Data API Standards <https://data-apis.org/>`_
in order to enable device aware data interchange between array/tensor
libraries in the Python ecosystem. See more about the standard
in the
`protocol documentation <https://data-apis.org/array-api/latest/index.html>`_
and more about DLPack in the
`Python Specification for DLPack <https://dmlc.github.io/dlpack/latest/python_spec.html#python-spec>`_.

Implementation of DLPack in PyArrow
-----------------------------------

The protocol is implemented for ``pa.Array`` and ``pa.Tensor`` with different behaviors.
``pa.Tensor`` can produce and consume all shapes and strides of a generic DLPack tensor.
``pa.Array`` on the other hand is purposely limited to produce and consume 1-dimensional
contiguous tensors (where the only dimension is the array's length).
The only exception is ``pa.FixedShapeTensorArray``, which is designed to represent
tensors and supports more generic shapes and strides.
It can produce and consume DLPack tensors whose outermost dimension has the largest
stride, that dimension being mapped to the array's length.

For both ``pa.Tensor`` and ``pa.Array``, only numeric data types are supported: integer,
unsigned integer and float.

Some array types can be understood as some form of tensor.
For instance, a nested fixed size list of a numeric data type has the same memory
representation as a row major tensor.
It is possible to get a (zero-copy) tensor from such an array using
``array.to_tensor()``, and then use DLPack on the resulting tensor.

The DLPack protocol fails on arrays with nulls, though these can be ignored with
an explicit conversion to a tensor using ``array.to_tensor(allow_nulls=True)``.
In that case, the null entries hold an unspecified value.
This is free, compared to ``pa.compute.fill_null`` which explicitly modifies the
array data to replace the null values.

Currently, the Arrow implementation of the protocol only supports
data on a CPU device.

Data interchange syntax of the protocol includes

1. ``from_dlpack(x, /, *, device=None, copy=None)``: consuming an array object that
   implements a ``__dlpack__`` method and creating a new array while sharing the
   memory.

2. ``__dlpack__(self, *, stream=None, max_version=None, dl_device=None, copy=None)``
   and ``__dlpack_device__``:
   producing a PyCapsule with the DLPack struct which is called from
   within ``from_dlpack(x)``.
   This method is intended for library authors.

Examples
--------

Producing
~~~~~~~~~

Convert a PyArrow CPU array into a NumPy array:

.. code-block:: python

    >>> import pyarrow as pa
    >>> import numpy as np
    >>> array = pa.array([2, 0, 2, 4])
    >>> array
    <pyarrow.lib.Int64Array object at ...>
    [
      2,
      0,
      2,
      4
    ]
    >>> np.from_dlpack(array)
    array([2, 0, 2, 4])

Convert a PyArrow CPU array into a PyTorch tensor:

.. code-block:: python

    >>> import torch  # doctest: +SKIP
    >>> torch.from_dlpack(array)  # doctest: +SKIP
    tensor([2, 0, 2, 4])

Convert a PyArrow CPU array into a JAX array:

.. code-block:: python

    >>> import jax  # doctest: +SKIP
    >>> jax.numpy.from_dlpack(array)  # doctest: +SKIP
    Array([2, 0, 2, 4], dtype=int32)
    >>> jax.dlpack.from_dlpack(array)  # doctest: +SKIP
    Array([2, 0, 2, 4], dtype=int32)

Arrays with a tensor memory layout, such as fixed size lists of a numeric type, need an
explicit conversion to a ``pa.Tensor``, which exports its full multi-dimensional shape:

.. code-block:: python

    >>> list_array = pa.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
    ...                       pa.list_(pa.float64(), 2))
    >>> list_array.to_tensor()
    <pyarrow.Tensor>
    type: double
    shape: (3, 2)
    strides: (16, 8)
    >>> np.from_dlpack(list_array.to_tensor())
    array([[1., 2.],
           [3., 4.],
           [5., 6.]])

A ``pa.FixedShapeTensorArray`` exports directly, the array length becoming the outermost
dimension, followed by the shape of the element tensors:

.. code-block:: python

    >>> nested = pa.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
    ...                   pa.list_(pa.list_(pa.int32(), 2), 2))
    >>> tensor_array = pa.FixedShapeTensorArray.from_tensor(nested.to_tensor())
    >>> tensor_array.type
    FixedShapeTensorType(extension<arrow.fixed_shape_tensor[value_type=int32, shape=[2,2], permutation=[0,1]]>)
    >>> np.from_dlpack(tensor_array).shape
    (2, 2, 2)

Arrays with nulls are rejected, unless the conversion to a tensor explicitly allows
them, in which case the null entries hold an unspecified value:

.. code-block:: python

    >>> array_with_nulls = pa.array([2, None, 4], pa.int32())
    >>> np.from_dlpack(array_with_nulls)
    Traceback (most recent call last):
        ...
    pyarrow.lib.ArrowTypeError: Can only use DLPack on arrays with no nulls.
    >>> np.from_dlpack(array_with_nulls.to_tensor(allow_nulls=True))
    array([2, ..., 4], dtype=int32)

Consuming
~~~~~~~~~

Any object implementing the DLPack protocol can be imported, without copying the data:

.. code-block:: python

    >>> pa.Array.from_dlpack(np.array([2, 0, 2, 4]))
    <pyarrow.lib.Int64Array object at ...>
    [
      2,
      0,
      2,
      4
    ]
    >>> pa.Tensor.from_dlpack(np.array([[2, 0], [2, 4]], np.int32))
    <pyarrow.Tensor>
    type: int32
    shape: (2, 2)
    strides: (8, 4)

``pa.Array.from_dlpack`` only accepts 1-dimensional contiguous tensors.
Multi-dimensional data can be imported as a ``pa.FixedShapeTensorArray``, the outermost
dimension becoming the length of the array:

.. code-block:: python

    >>> array = pa.FixedShapeTensorArray.from_dlpack(
    ...     np.arange(12, dtype=np.int32).reshape(3, 2, 2)
    ... )
    >>> array.type
    FixedShapeTensorType(extension<arrow.fixed_shape_tensor[value_type=int32, shape=[2,2], permutation=[0,1]]>)
    >>> len(array)
    3

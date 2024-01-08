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

The producing side of the DLPack Protocol is implemented for ``pa.Array``
and can be used to interchange data between PyArrow and other tensor
libraries. Supported data types are integer, unsigned integer and float. The
protocol has no missing data support meaning PyArrow arrays with
missing values cannot be transferred through the DLPack
protocol. Currently, the Arrow implementation of the protocol only supports
data on a CPU device.

Data interchange syntax of the protocol includes

1. ``from_dlpack(x)``: consuming an array object that implements a
   ``__dlpack__`` method and creating a new array while sharing the
   memory.

2. ``__dlpack__(self, stream=None)`` and ``__dlpack_device__``:
   producing a PyCapsule with the DLPack struct which is called from
   within ``from_dlpack(x)``.

PyArrow implements the second part of the protocol
(``__dlpack__(self, stream=None)`` and ``__dlpack_device__``) and can
thus be consumed by libraries implementing ``from_dlpack``.

Example
-------

Convert a PyArrow CPU array to NumPy array:

.. code-block::

    >>> import pyarrow as pa
    >>> array = pa.array([2, 0, 2, 4])
    <pyarrow.lib.Int64Array object at 0x121fd4880>
    [
    2,
    0,
    2,
    4
    ]

    >>> import numpy as np
    >>> np.from_dlpack(array)
    array([2, 0, 2, 4])

Convert a PyArrow CPU array to PyTorch tensor:

.. code-block::

    >>> import torch
    >>> torch.from_dlpack(array)
    tensor([2, 0, 2, 4])    

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

Producing side of the DLPack Protocol is implemented for ``pa.Array``
and can be used to interchange data between PyArrow and other tensor
libraries. The data structures that are supported in the implementation
of the protocol are integer, unsigned integer and float arrays. The
protocol has no missing data support meaning PyArrow arrays with
validity mask can not be used to transfer data through the DLPack
protocol. Currently Arrow implementation of the protocol only supports
data on a CPU device.

The DLPack Protocol is
`selected as the Python array API standard <https://data-apis.org/array-api/latest/design_topics/data_interchange.html?highlight=dlpack#dlpack-an-in-memory-tensor-structure>`_
by the
`Consortium for Python Data API Standards <https://data-apis.org/>`_
in order to enable device aware data interchange between array/tensor
libraries in the Python ecosystem. Being device aware allows exchange
of data on devices other than the CPU (e.g. GPU). See more about the standard
in the
`protocol documentation <https://data-apis.org/array-api/latest/index.html>`_
and more about the DLPack in the
`Python Specification for DLPack <https://dmlc.github.io/dlpack/latest/python_spec.html#python-spec>`_.

Data interchange syntax of the protocol includes

1. ``from_dlpack(x)``: consuming an array object that implements a ``__dlpack__`å` method
   and creating a new array while sharing the memory.

2. ``__dlpack__(self, stream=None)`` and ``__dlpack_device__``: producing a PyCapsule with
   the DLPack struct which is called from within ``from_dlpack(x)``.

PyArrow implements the second part of the protocol (``__dlpack__(self, stream=None)`` and
``__dlpack_device__``).

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

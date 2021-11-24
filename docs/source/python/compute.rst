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

.. currentmodule:: pyarrow.compute
.. _compute:

=================
Compute Functions
=================

Arrow supports logical compute operations over inputs of possibly
varying types.  Many compute functions support both array (chunked or not)
and scalar inputs, but some will mandate either.  For example,
``sort_indices`` requires its first and only input to be an array.

Below are a few simple examples:

   >>> import pyarrow as pa
   >>> import pyarrow.compute as pc
   >>> a = pa.array([1, 1, 2, 3])
   >>> pc.sum(a)
   <pyarrow.Int64Scalar: 7>
   >>> b = pa.array([4, 1, 2, 8])
   >>> pc.equal(a, b)
   <pyarrow.lib.BooleanArray object at 0x7f686e4eef30>
   [
     false,
     true,
     true,
     false
   ]
   >>> x, y = pa.scalar(7.8), pa.scalar(9.3)
   >>> pc.multiply(x, y)
   <pyarrow.DoubleScalar: 72.54>

These functions can do more than just element-by-element operations.
Here is an example of sorting a table:

    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> t = pa.table({'x':[1,2,3],'y':[3,2,1]})
    >>> i = pc.sort_indices(t, sort_keys=[('y', 'ascending')])
    >>> i
    <pyarrow.lib.UInt64Array object at 0x7fcee5df75e8>
    [
      2,
      1,
      0
    ]



.. seealso::

   :ref:`Available compute functions (C++ documentation) <compute-function-list>`.

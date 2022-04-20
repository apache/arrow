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
varying types.  

The standard compute operations are provided by the :mod:`pyarrow.compute`
module and can be used directly::

   >>> import pyarrow as pa
   >>> import pyarrow.compute as pc
   >>> a = pa.array([1, 1, 2, 3])
   >>> pc.sum(a)
   <pyarrow.Int64Scalar: 7>

The grouped aggregation functions raise an exception instead
and need to be used through the :meth:`pyarrow.Table.group_by` capabilities.
See :ref:`py-grouped-aggrs` for more details.

Standard Compute Functions
==========================

Many compute functions support both array (chunked or not)
and scalar inputs, but some will mandate either.  For example,
``sort_indices`` requires its first and only input to be an array.

Below are a few simple examples::

   >>> import pyarrow as pa
   >>> import pyarrow.compute as pc
   >>> a = pa.array([1, 1, 2, 3])
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
Here is an example of sorting a table::

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

For a complete list of the compute functions that PyArrow provides
you can refer to :ref:`api.compute` reference.

.. seealso::

   :ref:`Available compute functions (C++ documentation) <compute-function-list>`.

.. _py-grouped-aggrs:

Grouped Aggregations
====================

PyArrow supports grouped aggregations over :class:`pyarrow.Table` through the
:meth:`pyarrow.Table.group_by` method. 
The method will return a grouping declaration
to which the hash aggregation functions can be applied::

   >>> import pyarrow as pa
   >>> t = pa.table([
   ...       pa.array(["a", "a", "b", "b", "c"]),
   ...       pa.array([1, 2, 3, 4, 5]),
   ... ], names=["keys", "values"])
   >>> t.group_by("keys").aggregate([("values", "sum")])
   pyarrow.Table
   values_sum: int64
   keys: string
   ----
   values_sum: [[3,7,5]]
   keys: [["a","b","c"]]

The ``"sum"`` aggregation passed to the ``aggregate`` method in the previous
example is the ``hash_sum`` compute function.

Multiple aggregations can be performed at the same time by providing them
to the ``aggregate`` method::

   >>> import pyarrow as pa
   >>> t = pa.table([
   ...       pa.array(["a", "a", "b", "b", "c"]),
   ...       pa.array([1, 2, 3, 4, 5]),
   ... ], names=["keys", "values"])
   >>> t.group_by("keys").aggregate([
   ...    ("values", "sum"),
   ...    ("keys", "count")
   ... ])
   pyarrow.Table
   values_sum: int64
   keys_count: int64
   keys: string
   ----
   values_sum: [[3,7,5]]
   keys_count: [[2,2,1]]
   keys: [["a","b","c"]]

Aggregation options can also be provided for each aggregation function,
for example we can use :class:`CountOptions` to change how we count
null values::

   >>> import pyarrow as pa
   >>> import pyarrow.compute as pc
   >>> table_with_nulls = pa.table([
   ...    pa.array(["a", "a", "a"]),
   ...    pa.array([1, None, None])
   ... ], names=["keys", "values"])
   >>> table_with_nulls.group_by(["keys"]).aggregate([
   ...    ("values", "count", pc.CountOptions(mode="all"))
   ... ])
   pyarrow.Table
   values_count: int64
   keys: string
   ----
   values_count: [[3]]
   keys: [["a"]]
   >>> table_with_nulls.group_by(["keys"]).aggregate([
   ...    ("values", "count", pc.CountOptions(mode="only_valid"))
   ... ])
   pyarrow.Table
   values_count: int64
   keys: string
   ----
   values_count: [[1]]
   keys: [["a"]]

Following is a list of all supported grouped aggregation functions.
You can use them with or without the ``"hash_"`` prefix.

.. arrow-computefuncs::
  :kind: hash_aggregate

.. _py-joins:

Table and Dataset Joins
=======================

Both :class:`.Table` and :class:`.Dataset` support
join operations through :meth:`.Table.join`
and :meth:`.Dataset.join` methods.

The methods accept a right table or dataset that will
be joined to the initial one and one or more keys that
should be used from the two entities to perform the join.

By default a ``left outer join`` is performed, but it's possible
to ask for any of the supported join types:

* left semi
* right semi
* left anti
* right anti
* inner
* left outer
* right outer
* full outer

A basic join can be performed just by providing a table and a key
on which the join should be performed:

.. code-block:: python

   import pyarrow as pa

   table1 = pa.table({'id': [1, 2, 3],
                      'year': [2020, 2022, 2019]})

   table2 = pa.table({'id': [3, 4],
                      'n_legs': [5, 100],
                      'animal': ["Brittle stars", "Centipede"]})

   joined_table = table1.join(table2, keys="id")

The result will be a new table created by joining ``table1`` with
``table2`` on the ``id`` key with a ``left outer join``::

   pyarrow.Table
   id: int64
   year: int64
   n_legs: int64
   animal: string
   ----
   id: [[3,1,2]]
   year: [[2019,2020,2022]]
   n_legs: [[5,null,null]]
   animal: [["Brittle stars",null,null]]

We can perform additional type of joins, like ``full outer join`` by
passing them to the ``join_type`` argument:

.. code-block:: python

   table1.join(table2, keys='id', join_type="full outer")

In that case the result would be::

   pyarrow.Table
   id: int64
   year: int64
   n_legs: int64
   animal: string
   ----
   id: [[3,1,2],[4]]
   year: [[2019,2020,2022],[null]]
   n_legs: [[5,null,null],[100]]
   animal: [["Brittle stars",null,null],["Centipede"]]

It's also possible to provide additional join keys, so that the
join happens on two keys instead of one. For example we can add
an ``year`` column to ``table2`` so that we can join on ``('id', 'year')``:

.. code-block::

   table2_withyear = table2.append_column("year", pa.array([2019, 2022]))
   table1.join(table2_withyear, keys=["id", "year"])

The result will be a table where only entries with ``id=3`` and ``year=2019``
have data, the rest will be ``null``::

   pyarrow.Table
   id: int64
   year: int64
   animal: string
   n_legs: int64
   ----
   id: [[3,1,2]]
   year: [[2019,2020,2022]]
   animal: [["Brittle stars",null,null]]
   n_legs: [[5,null,null]]

The same capabilities are available for :meth:`.Dataset.join` too, so you can
take two datasets and join them:

.. code-block::

   import pyarrow.dataset as ds

   ds1 = ds.dataset(table1)
   ds2 = ds.dataset(table2)

   joined_ds = ds1.join(ds2, key="id")

The resulting dataset will be an :class:`.InMemoryDataset` containing the joined data::

   >>> joined_ds.head(5)

   pyarrow.Table
   id: int64
   year: int64
   animal: string
   n_legs: int64
   ----
   id: [[3,1,2]]
   year: [[2019,2020,2022]]
   animal: [["Brittle stars",null,null]]
   n_legs: [[5,null,null]]



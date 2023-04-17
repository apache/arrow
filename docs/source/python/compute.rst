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
   id: [[3,1,2,4]]
   year: [[2019,2020,2022,null]]
   n_legs: [[5,null,null,100]]
   animal: [["Brittle stars",null,null,"Centipede"]]

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

.. _py-filter-expr:

Filtering by Expressions
========================

:class:`.Table` and :class:`.Dataset` can
both be filtered using a boolean :class:`.Expression`.

The expression can be built starting from a 
:func:`pyarrow.compute.field`. Comparisons and transformations
can then be applied to one or more fields to build the filter
expression you care about.

Most :ref:`compute` can be used to perform transformations
on a ``field``.

For example we could build a filter to find all rows that are even
in column ``"nums"``

.. code-block:: python

   import pyarrow.compute as pc
   even_filter = (pc.bit_wise_and(pc.field("nums"), pc.scalar(1)) == pc.scalar(0))

.. note::

   The filter finds even numbers by performing a bitwise and operation between the number and ``1``.
   As ``1`` is to ``00000001`` in binary form, only numbers that have the last bit set to ``1``
   will return a non-zero result from the ``bit_wise_and`` operation. This way we are identifying all
   odd numbers. Given that we are interested in the even ones, we then check that the number returned
   by the ``bit_wise_and`` operation equals ``0``. Only the numbers where the last bit was ``0`` will
   return a ``0`` as the result of ``num & 1`` and as all numbers where the last bit is ``0`` are
   multiples of ``2`` we will be filtering for the even numbers only.
   
Once we have our filter, we can provide it to the :meth:`.Table.filter` method
to filter our table only for the matching rows:

.. code-block:: python

   >>> table = pa.table({'nums': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
   ...                   'chars': ["a", "b", "c", "d", "e", "f", "g", "h", "i", "l"]})
   >>> table.filter(even_filter)
   pyarrow.Table
   nums: int64
   chars: string
   ----
   nums: [[2,4,6,8,10]]
   chars: [["b","d","f","h","l"]]

Multiple filters can be joined using ``&``, ``|``, ``~`` to perform ``and``, ``or``
and ``not`` operations. For example using ``~even_filter`` will actually end up filtering
for all numbers that are odd:

.. code-block:: python

   >>> table.filter(~even_filter)
   pyarrow.Table
   nums: int64
   chars: string
   ----
   nums: [[1,3,5,7,9]]
   chars: [["a","c","e","g","i"]]

and we could build a filter that finds all even numbers greater than 5 by combining
our ``even_filter`` with a ``pc.field("nums") > 5`` filter:

.. code-block:: python

   >>> table.filter(even_filter & (pc.field("nums") > 5))
   pyarrow.Table
   nums: int64
   chars: string
   ----
   nums: [[6,8,10]]
   chars: [["f","h","l"]]

:class:`.Dataset` can similarly be filtered with the :meth:`.Dataset.filter` method.
The method will return an instance of :class:`.Dataset` which will lazily
apply the filter as soon as actual data of the dataset is accessed:

   >>> dataset = ds.dataset(table)
   >>> filtered = dataset.filter(pc.field("nums") < 5).filter(pc.field("nums") > 2)
   >>> filtered.to_table()
   pyarrow.Table
   nums: int64
   chars: string
   ----
   nums: [[3,4]]
   chars: [["c","d"]]


User-Defined Functions
======================

.. warning::
   This API is **experimental**.

PyArrow allows defining and registering custom compute functions.
These functions can then be called from Python as well as C++ (and potentially
any other implementation wrapping Arrow C++, such as the R ``arrow`` package)
using their registered function name. 

UDF support is limited to scalar functions. A scalar function is a function which
executes elementwise operations on arrays or scalars. In general, the output of a
scalar function does not depend on the order of values in the arguments. Note that
such functions have a rough correspondence to the functions used in SQL expressions,
or to NumPy `universal functions <https://numpy.org/doc/stable/reference/ufuncs.html>`_.

To register a UDF, a function name, function docs, input types and
output type need to be defined. Using :func:`pyarrow.compute.register_scalar_function`,

.. code-block:: python

   import numpy as np

   import pyarrow as pa
   import pyarrow.compute as pc

   function_name = "numpy_gcd"
   function_docs = {
         "summary": "Calculates the greatest common divisor",
         "description":
            "Given 'x' and 'y' find the greatest number that divides\n"
            "evenly into both x and y."
   }

   input_types = {
      "x" : pa.int64(),
      "y" : pa.int64()
   }

   output_type = pa.int64()

   def to_np(val):
       if isinstance(val, pa.Scalar):
          return val.as_py()
       else:
          return np.array(val)

   def gcd_numpy(ctx, x, y):
       np_x = to_np(x)
       np_y = to_np(y)
       return pa.array(np.gcd(np_x, np_y))

   pc.register_scalar_function(gcd_numpy,
                              function_name,
                              function_docs,
                              input_types,
                              output_type)
   

The implementation of a user-defined function always takes a first *context*
parameter (named ``ctx`` in the example above) which is an instance of
:class:`pyarrow.compute.ScalarUdfContext`.
This context exposes several useful attributes, particularly a
:attr:`~pyarrow.compute.ScalarUdfContext.memory_pool` to be used for
allocations in the context of the user-defined function.

You can call a user-defined function directly using :func:`pyarrow.compute.call_function`:

.. code-block:: python

   >>> pc.call_function("numpy_gcd", [pa.scalar(27), pa.scalar(63)])
   <pyarrow.Int64Scalar: 9>
   >>> pc.call_function("numpy_gcd", [pa.scalar(27), pa.array([81, 12, 5])])
   <pyarrow.lib.Int64Array object at 0x7fcfa0e7b100>
   [
     27,
     3,
     1
   ]

Working with Datasets
---------------------

More generally, user-defined functions are usable everywhere a compute function
can be referred by its name. For example, they can be called on a dataset's
column using :meth:`Expression._call`.

Consider an instance where the data is in a table and we want to compute
the GCD of one column with the scalar value 30.  We will be re-using the
"numpy_gcd" user-defined function that was created above:

.. code-block:: python

   >>> import pyarrow.dataset as ds
   >>> data_table = pa.table({'category': ['A', 'B', 'C', 'D'], 'value': [90, 630, 1827, 2709]})
   >>> dataset = ds.dataset(data_table)
   >>> func_args = [pc.scalar(30), ds.field("value")]
   >>> dataset.to_table(
   ...             columns={
   ...                 'gcd_value': ds.field('')._call("numpy_gcd", func_args),
   ...                 'value': ds.field('value'),
   ...                 'category': ds.field('category')
   ...             })
   pyarrow.Table
   gcd_value: int64
   value: int64
   category: string
   ----
   gcd_value: [[30,30,3,3]]
   value: [[90,630,1827,2709]]
   category: [["A","B","C","D"]]

Note that ``ds.field('')_call(...)`` returns a :func:`pyarrow.compute.Expression`.
The arguments passed to this function call are expressions, not scalar values 
(notice the difference between :func:`pyarrow.scalar` and :func:`pyarrow.compute.scalar`,
the latter produces an expression). 
This expression is evaluated when the projection operator executes it.

Projection Expressions
^^^^^^^^^^^^^^^^^^^^^^
In the above example we used an expression to add a new column (``gcd_value``)
to our table.  Adding new, dynamically computed, columns to a table is known as "projection"
and there are limitations on what kinds of functions can be used in projection expressions.
A projection function must emit a single output value for each input row.  That output value
should be calculated entirely from the input row and should not depend on any other row.
For example, the "numpy_gcd" function that we've been using as an example above is a valid
function to use in a projection.  A "cumulative sum" function would not be a valid function
since the result of each input row depends on the rows that came before.  A "drop nulls"
function would also be invalid because it doesn't emit a value for some rows.

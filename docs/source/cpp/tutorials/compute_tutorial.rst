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

.. default-domain:: cpp
.. highlight:: cpp

.. cpp:namespace:: arrow

=============
Arrow Compute
=============

Apache Arrow provides compute functions to facilitate efficient and
portable data processing. In this article, you will use Arrow’s compute
functionality to:

1. Calculate a sum over a column

2. Calculate element-wise sums over two columns

3. Search for a value in a column

Pre-requisites 
---------------

Before continuing, make sure you have:

1. An Arrow installation, which you can set up here: :doc:`/cpp/build_system`

2. An understanding of basic Arrow data structures from :doc:`/cpp/tutorials/basic_arrow`

Setup
-----

Before running some computations, we need to fill in a couple gaps:

1. We need to include necessary headers.
   
2. ``A main()`` is needed to glue things together.

3. We need data to play with.
   
Includes
^^^^^^^^

Before writing C++ code, we need some includes. We'll get ``iostream`` for output, then import Arrow's 
compute functionality: 

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Includes)
  :end-before: (Doc section: Includes)

Main()
^^^^^^

For our glue, we’ll use the ``main()`` pattern from the previous tutorial on
data structures:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Main)
  :end-before: (Doc section: Main)

Which, like when we used it before, is paired with a ``RunMain()``:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: RunMain)
  :end-before: (Doc section: RunMain)

Generating Tables for Computation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before we begin, we’ll initialize a :class:`Table` with two columns to play with. We’ll use
the method from :doc:`/cpp/tutorials/basic_arrow`, so look back
there if anything’s confusing:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Create Tables)
  :end-before: (Doc section: Create Tables)

Calculating a Sum over an Array
-------------------------------

Using a computation function has two general steps, which we separate
here:

1. Preparing a :class:`Datum` for output

2. Calling :func:`compute::Sum`, a convenience function for summation over an :class:`Array`

3. Retrieving and printing output

Prepare Memory for Output with Datum
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When computation is done, we need somewhere for our results to go. In
Arrow, the object for such output is called :class:`Datum`. This object is used
to pass around inputs and outputs in compute functions, and can contain
many differently-shaped Arrow data structures. We’ll need it to retrieve
the output from compute functions.

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Sum Datum Declaration)
  :end-before: (Doc section: Sum Datum Declaration)

Call Sum()
^^^^^^^^^^

Here, we’ll get our :class:`Table`, which has columns “A” and “B”, and sum over
column “A.” For summation, there is a convenience function, called
:func:`compute::Sum`, which reduces the complexity of the compute interface. We’ll look
at the more complex version for the next computation. For a given
function, refer to :doc:`/cpp/api/compute` to see if there is a
convenience function. :func:`compute::Sum` takes in a given :class:`Array` or :class:`ChunkedArray`
– here, we use :func:`Table::GetColumnByName` to pass in column A. Then, it outputs to
a :class:`Datum`. Putting that all together, we get this:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Sum Call)
  :end-before: (Doc section: Sum Call)

Get Results from Datum
^^^^^^^^^^^^^^^^^^^^^^

The previous step leaves us with a :class:`Datum` which contains our sum.
However, we cannot print it directly – its flexibility in holding
arbitrary Arrow data structures means we have to retrieve our data
carefully. First, to understand what’s in it, we can check which kind of
data structure it is, then what kind of primitive is being held:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Sum Datum Type)
  :end-before: (Doc section: Sum Datum Type)

This should report the :class:`Datum` stores a :class:`Scalar` with a 64-bit integer. Just
to see what the value is, we can print it out like so, which yields
12891:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Sum Contents)
  :end-before: (Doc section: Sum Contents)

Now we’ve used :func:`compute::Sum` and gotten what we want out of it!

Calculating Element-Wise Array Addition with CallFunction()
-----------------------------------------------------------

A next layer of complexity uses what :func:`compute::Sum` was helpfully hiding:
:func:`compute::CallFunction`. For this example, we will explore how to use the more
robust :func:`compute::CallFunction` with the “add” compute function. The pattern
remains similar:

1. Preparing a Datum for output

2. Calling :func:`compute::CallFunction` with “add”

3. Retrieving and printing output

Prepare Memory for Output with Datum
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once more, we’ll need a Datum for any output we get:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Add Datum Declaration)
  :end-before: (Doc section: Add Datum Declaration)

Use CallFunction() with “add”
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:func:`compute::CallFunction` takes the name of the desired function as its first
argument, then the data inputs for said function as a vector in its
second argument. Right now, we want an element-wise addition between
columns “A” and “B”. So, we’ll ask for “add,” pass in columns “A and B”,
and output to our :class:`Datum`. Put this all together, and we get:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Add Call)
  :end-before: (Doc section: Add Call)

.. seealso:: :ref:`compute-function-list` for a list of other functions to go with :func:`compute::CallFunction`

Get Results from Datum
^^^^^^^^^^^^^^^^^^^^^^

Again, the :class:`Datum` needs some careful handling. Said handling is much
easier when we know what’s in it. This :class:`Datum` holds a :class:`ChunkedArray` with
32-bit integers, but we can print that to confirm:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Add Datum Type)
  :end-before: (Doc section: Add Datum Type)

Since it’s a :class:`ChunkedArray`, we request that from the :class:`Datum` – :class:`ChunkedArray`
has a :func:`ChunkedArray::ToString` method, so we’ll use that to print out its contents:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Add Contents)
  :end-before: (Doc section: Add Contents)

The output looks like this::

  Datum kind: ChunkedArray content type: int32
  [
    [
      75376,
      647,
      2287,
      5671,
      5092
    ]
  ]

Now, we’ve used :func:`compute::CallFunction`, instead of a convenience function! This
enables a much wider range of available computations.

Searching for a Value with CallFunction() and Options
-----------------------------------------------------

One class of computations remains. :func:`compute::CallFunction` uses a vector for data
inputs, but computation often needs additional arguments to function. In
order to supply this, computation functions may be associated with
structs where their arguments can be defined. You can check a given
function to see which struct it uses :ref:`here <compute-function-list>`. For this example, we’ll search for a value in column “A” using
the “index” compute function. This process has three steps, as opposed
to the two from before:

1. Preparing a :class:`Datum` for output

2. Preparing :class:`compute::IndexOptions`

3. Calling :func:`compute::CallFunction` with “index” and :class:`compute::IndexOptions`

4. Retrieving and printing output

Prepare Memory for Output with Datum
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We’ll need a :class:`Datum` for any output we get:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Index Datum Declare)
  :end-before: (Doc section: Index Datum Declare)

Configure “index” with IndexOptions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For this exploration, we’ll use the “index” function – this is a
searching method, which returns the index of an input value. In order to
pass this input value, we require an :class:`compute::IndexOptions` struct. So, let’s make
that struct:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: IndexOptions Declare)
  :end-before: (Doc section: IndexOptions Declare)

In a searching function, one requires a target value. Here, we’ll use
2223, the third item in column A, and configure our struct accordingly:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: IndexOptions Assign)
  :end-before: (Doc section: IndexOptions Assign)

Use CallFunction() with “index” and IndexOptions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To actually run the function, we use :func:`compute::CallFunction` again, this time
passing our IndexOptions struct by reference as a third argument. As
before, the first argument is the name of the function, and the second
our data input:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Index Call)
  :end-before: (Doc section: Index Call)

Get Results from Datum
^^^^^^^^^^^^^^^^^^^^^^

One last time, let’s see what our :class:`Datum` has! This will be a :class:`Scalar` with
a 64-bit integer, and the output will be 2:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Index Inspection)
  :end-before: (Doc section: Index Inspection)

Ending Program
--------------

At the end, we just return :func:`arrow::Status::OK`, so the ``main()`` knows that
we’re done, and that everything’s okay, just like the preceding
tutorials.

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Ret)
  :end-before: (Doc section: Ret)

With that, you’ve used compute functions which fall into the three main
types – with and without convenience functions, then with an Options
struct. Now you can process any :class:`Table` you need to, and solve whatever
data problem you have that fits into memory!

Which means that now we have to see how we can work with
larger-than-memory datasets, via Arrow Datasets in the next article.

Refer to the below for a copy of the complete code:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/compute_example.cc
  :language: cpp
  :start-after: (Doc section: Compute Example)
  :end-before: (Doc section: Compute Example)
  :linenos:
  :lineno-match:
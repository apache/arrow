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

1. An Arrow installation

2. An understanding of basic Arrow data structures from <basic data structures>

Setup
-----

Before running some computations, we need to fill in a couple gaps:

1. A main() is needed to glue things together.

2. We need data to play with.

Main()
^^^^^^

For our glue, we’ll use the main() pattern from the previous tutorial on
data structures:

<MAIN WITH STATUS CHECKER>

Which, like when we used it before, is paired with a RunMain():

<RUNMAIN START>

Generating Tables for Computation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First, we’ll initialize a Table with two columns to play with. We’ll use
the method from the Arrow data structure tutorial<link>, so look back
there if anything’s confusing:

<TABLE CREATION>

Calculating a Sum over an Array
-------------------------------

Using a computation function has two general steps, which we separate
here:

1. Preparing a Datum for output

2. Calling Sum(), a convenience function for summation over an Array

3. Retrieving and printing output

Prepare Memory for Output with Datum
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When computation is done, we need somewhere for our results to go. In
Arrow, the object for such output is called Datum. This object is used
to pass around inputs and outputs in compute functions, and can contain
many differently-shaped Arrow data structures. We’ll need it to retrieve
the output from compute functions.

<DATUM DECLARATION>

Call Sum()
^^^^^^^^^^

Here, we’ll get our Table, which has columns “A” and “B”, and sum over
column “A.” For summation, there is a convenience function, called
Sum(), which reduces the complexity of the compute interface. We’ll look
at the more complex version for the next computation. For a given
function, refer to the API reference<link> to see if there is a
convenience function. Sum() takes in a given Arrow Array or ChunkedArray
– here, we use GetColumnByName to pass in column A. Then, it outputs to
a Datum. Putting that all together, we get this:

<SUM CALL>

Get Results from Datum
^^^^^^^^^^^^^^^^^^^^^^

The previous step leaves us with a Datum which contains our sum.
However, we cannot print it directly – its flexibility in holding
arbitrary Arrow data structures means we have to retrieve our data
carefully. First, to understand what’s in it, we can check which kind of
data structure it is, then what kind of primitive is being held:

<PRINT OUT DATA KIND AND TYPE>

This should report the Datum stores a Scalar with a 64-bit integer. Just
to see what the value is, we can print it out like so, which yields
12891:

<PRINT OUT WITH SCALAR AS>

Now we’ve used Sum() and gotten what we want out of it!

Calculating Element-Wise Array Addition with CallFunction()
-----------------------------------------------------------

A next layer of complexity uses what Sum() was helpfully hiding:
CallFunction(). For this example, we will explore how to use the more
robust CallFunction() with the “add” compute function. The pattern
remains similar:

1. Preparing a Datum for output

2. Calling CallFunction() with “add”

3. Retrieving and printing output

.. _prepare-memory-for-output-with-datum-1:

Prepare Memory for Output with Datum
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once more, we’ll need a Datum for any output we get:

<DATUM DECLARATION>

Use CallFunction() with “add”
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

CallFunction() takes the name of the desired function as its first
argument, then the data inputs for said function as a vector in its
second argument. Right now, we want an element-wise addition between
columns “A” and “B”. So, we’ll ask for “add,” pass in columns “A and B”,
and output to our Datum. Put this all together, and we get:

<CALL ADD>

.. _get-results-from-datum-1:

Get Results from Datum
^^^^^^^^^^^^^^^^^^^^^^

Again, the Datum needs some careful handling. Said handling is much
easier when we know what’s in it. This Datum holds a ChunkedArray with
32-bit integers, but we can print that to confirm:

<PRINT OUT DATA KIND AND TYPE>

Since it’s a ChunkedArray, we request that from the Datum – ChunkedArray
has a ToString() method, so we’ll use that to print out its contents:

<PRINT OUT CHUNKEDARRAY>

The output looks like this:

<TABLE WITH OUTPUTS 75376, 647, 2287, 5671, 5092>

Now, we’ve used CallFunction(), instead of a convenience function! This
enables a much wider range of available computations.

Searching for a Value with CallFunction() and Options
-----------------------------------------------------

One class of computations remains. CallFunction() uses a vector for data
inputs, but computation often needs additional arguments to function. In
order to supply this, computation functions may be associated with
structs where their arguments can be defined. You can check a given
function to see which struct it uses here: <link to compute function
list>. For this example, we’ll search for a value in column “A” using
the “index” compute function. This process has three steps, as opposed
to the two from before:

1. Preparing a Datum for output

2. Preparing IndexOptions

3. Calling CallFunction() with “index” and IndexOptions

4. Retrieving and printing output

.. _prepare-memory-for-output-with-datum-2:

Prepare Memory for Output with Datum
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once more, we’ll need a Datum for any output we get:

<DATUM DECLARATION>

Configure “index” with IndexOptions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For this exploration, we’ll use the “index” function – this is a
searching method, which returns the index of an input value. In order to
pass this input value, we require an IndexOptions struct. So, let’s make
that struct:

<INDEX OPTIONS DECLARATION>

In a searching function, one requires a target value. Here, we’ll use
2223, the third item in column A, and configure our struct accordingly:

<ASSIGN VALUE TO INDEXOPTIONS VALUE>

Use CallFunction() with “index” and IndexOptions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To actually run the function, we use CallFunction() again, this time
passing our IndexOptions struct by reference as a third argument. As
before, the first argument is the name of the function, and the second
our data input:

<CALLFUNCTION INDEX>

.. _get-results-from-datum-2:

Get Results from Datum
^^^^^^^^^^^^^^^^^^^^^^

One last time, let’s see what our Datum has! This will be a Scalar with
a 64-bit integer, and the output will be 2:

<DATUM INSPECTION>

Ending Program
--------------

At the end, we just return arrow::Status::OK(), so the main() knows that
we’re done, and that everything’s okay, just like the preceding
tutorials.

<RETURN STATEMENT>

With that, you’ve used compute functions which fall into the three main
types – with and without convenience functions, then with an Options
struct. Now you can process any Table you need to, and solve whatever
data problem you have that fits into memory!

Which means that now we have to see how we can work with
larger-than-memory datasets, via Arrow Datasets.

<LINK TO NEXT ARTICLE>

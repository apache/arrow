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

===========================
Basic Arrow Data Structures
===========================

Apache Arrow provides fundamental data structures for representing data:
:class:`arrow::Array`, :class:`arrow::ChunkedArray`, :class:`arrow::RecordBatch`, and :class:`arrow::Table`. This article shows how to
construct these data structures from primitive data types; specifically,
we will work with integers of varying size representing days, months,
and years. We will use them to create Arrow Arrays and ChunkedArrays,
and use those to construct tabular data structures, i.e., RecordBatch
and Table.

Setup
=====

First, we need a ``main()`` – a common pattern in Arrow looks like the
following:

<MAIN WITH STATUS CHECKER>

This allows us to easily use Arrow’s error-handling macros, which will
return back to ``main()`` with a :class:`arrow::Status` object if a failure occurs – and
this ``main()`` will report the error. Note that this means Arrow never
raises exceptions, instead relying upon returning Status. For more on
that, read here: <LINK TO CONCEPTUAL OVERVIEW>

To accompany this ``main()``, we have a ``RunMain()`` from which any Status
objects can return – this is where we’ll write the rest of the program:

<RUNMAIN START>

Making an Arrow Array
=====================

Next, we want to get the data from an array in standard C++ into an
Arrow Array. We still guarantee contiguity of memory in an Array, so no
worries about a performance loss when using Arrow Array vs C++ arrays.
The easiest way to construct an Arrow Array uses a Builder class. <RST
NOTE NEAR HERE: for more technical details, check out…> The following
code initializes a Builder for an Arrow Array that will hold 8 bit
integers, and uses the ``AppendValues()`` method, present in concrete 
:class:`arrow::ArrayBuilder` subclasses, to fill it with the
contents of a standard C++ array. Note the use of :c:macro:`ARROW_RETURN_NOT_OK`.
If ``AppendValues()`` fails, this macro will return to ``main()``, which will
print out the meaning of the failure.

<INT8BUILDER USE 1 APPEND>

Once a Builder has the values we want in our Array, we can use :func:`~arrow::ArrayBuilder::Finish`
to output the final structure to an Array – specifically, we output to a
std::shared_ptr<arrow::Array>. Note the use of :c:macro:`ARROW_ASSIGN_OR_RAISE`.
``Finish()`` outputs a :class:`arrow::Result` object, which :c:macro:`ARROW_ASSIGN_OR_RAISE` can
process. If the method fails, it will return to ``main()`` with a ``Status``
that will explain what went wrong. If it succeeds, then it will assign
the final output to the left-hand variable.

<INT8BUILDER USE 1 FINISH>

Once a Builder has had its ``Finish()`` method called, its state resets, so
it can be used again, like so:

<INT8BUILDER USE 2 APPEND+FINISH>

However, Builders cannot have their type changed in the middle of their
use – we have to make a new one when we switch to year data, which
requires a 16-bit integer at the minimum. Of course, there’s a Builder
for that:

<INT16BUILDER USE 1 APPEND+FINISH>

Now, we have three Arrow Arrays, with some variance in type.

Making a RecordBatch
====================

A columnar data format only really comes into play when you have a table. 
So, let’s make one. The first kind we’ll make is the RecordBatch – this 
uses Arrays internally, which means all data will be contiguous within each 
column, but any appending or concatenating will require copying.

To get started making a RecordBatch, we first need to define
characteristics of the columns, each represented by a :class:`arrow::Field` instance.
Each Field contains a name and datatype for its associated column; then,
a :class:`arrow::Schema` groups them together and sets the order of the columns, like
so:

<SCHEMA CREATION>

With data in Arrays and column descriptions in our Schema, we can make
the RecordBatch. Note that the length of the columns is necessary, and
the length is shared by all columns.

<RECORDBATCH CREATION>

Now, we have our data in a nice tabular form, safely within the RecordBatch.

Making a Table
==============

Let’s say that we want an array made up of sub-arrays, because it
can be useful for logic, for parallelizing work, for fitting each chunk
cutely into cache, or for exceeding the 2,147,483,647 row limit in a
standard Arrow Array. For this, Arrow offers ChunkedArray, which can be
made up of individual Arrow Arrays. So, let’s build a few more Arrays,
using the same builders for ease of use:

<THREE MORE ARRAYS CREATION>

Now, we can get into the magic. First, we’ll get an :class:`arrow::ArrayVector` (vector
of arrays):

<INITIALIZE ARRAYVECTOR>

This doesn’t have all the features of a ChunkedArray, so we don’t stop
here. Instead, we use it to create our ChunkedArray:

<CHUNKEDARRAY CREATION>

Now, we have a ChunkedArray for our day values. We’ll repeat that for
months and years:

<MONTH AND YEAR CHUNKEDARRAY CREATION>

Now, we have ChunkedArrays which contain our newly extended data in
bite-sized chunks. A RecordBatch is specifically for Arrays, and its
counterpart for ChunkedArrays is the Table. Its constructor is
effectively identical, it just happens to put the length of the columns
in position 3, and makes a Table. We re-use the Schema from before, and
make our Table:

<TABLE CREATION>

After that, we just return arrow::Status::OK(), so the main() knows that
we’re done, and that everything’s okay.

<RETURN STATEMENT>

With that, you’ve created the fundamental data structures in Arrow, and
can proceed to getting them in and out of a program with file I/O.

<LINK TO NEXT ARTICLE>

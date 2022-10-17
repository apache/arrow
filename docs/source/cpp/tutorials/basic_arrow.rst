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

===========================
Basic Arrow Data Structures
===========================

Apache Arrow provides fundamental data structures for representing data:
:class:`Array`, :class:`ChunkedArray`, :class:`RecordBatch`, and :class:`Table`. 
This article shows how to construct these data structures from primitive 
data types; specifically, we will work with integers of varying size 
representing days, months, and years. We will use them to create the following data structures:

#. Arrow :class:`Arrays <Array>`
#. :class:`ChunkedArrays<ChunkedArray>` 
#. :class:`RecordBatch`, from :class:`Arrays <Array>`
#. :class:`Table`, from :class:`ChunkedArrays<ChunkedArray>` 

Pre-requisites
--------------
Before continuing, make sure you have:

#. An Arrow installation, which you can set up here: :doc:`/cpp/build_system`
#. Understanding of how to use basic C++ data structures
#. Understanding of basic C++ data types


Setup
-----

Before trying out Arrow, we need to fill in a couple gaps:

1. We need to include necessary headers.
   
2. ``A main()`` is needed to glue things together.

Includes
^^^^^^^^

First, as ever, we need some includes. We'll get ``iostream`` for output, then import Arrow's basic
functionality from ``api.h``, like so: 

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: Includes)
  :end-before: (Doc section: Includes)

Main()
^^^^^^

Next, we need a ``main()`` – a common pattern with Arrow looks like the
following:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: Main)
  :end-before: (Doc section: Main)

This allows us to easily use Arrow’s error-handling macros, which will
return back to ``main()`` with a :class:`arrow::Status` object if a failure occurs – and
this ``main()`` will report the error. Note that this means Arrow never
raises exceptions, instead relying upon returning :class:`Status`. For more on
that, read here: :doc:`/cpp/conventions`.

To accompany this ``main()``, we have a ``RunMain()`` from which any :class:`Status`
objects can return – this is where we’ll write the rest of the program:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: RunMain Start)
  :end-before: (Doc section: RunMain Start)


Making an Arrow Array
---------------------

Building int8 Arrays
^^^^^^^^^^^^^^^^^^^^

Given that we have some data in standard C++ arrays, and want to use Arrow, we need to move
the data from said arrays into Arrow arrays. We still guarantee contiguity of memory in an 
:class:`Array`, so no worries about a performance loss when using :class:`Array` vs C++ arrays.
The easiest way to construct an :class:`Array` uses an :class:`ArrayBuilder`. 

.. seealso:: :doc:`/cpp/arrays` for more technical details on :class:`Array`

The following code initializes an :class:`ArrayBuilder` for an :class:`Array` that will hold 8 bit
integers. Specifically, it uses the ``AppendValues()`` method, present in concrete 
:class:`arrow::ArrayBuilder` subclasses, to fill the :class:`ArrayBuilder` with the
contents of a standard C++ array. Note the use of :c:macro:`ARROW_RETURN_NOT_OK`.
If ``AppendValues()`` fails, this macro will return to ``main()``, which will
print out the meaning of the failure.

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: int8builder 1 Append)
  :end-before: (Doc section: int8builder 1 Append)

Given an :class:`ArrayBuilder` has the values we want in our :class:`Array`, we can use 
:func:`ArrayBuilder::Finish` to output the final structure to an :class:`Array` – specifically, 
we output to a ``std::shared_ptr<arrow::Array>``. Note the use of :c:macro:`ARROW_ASSIGN_OR_RAISE`
in the following code. :func:`~ArrayBuilder::Finish` outputs a :class:`arrow::Result` object, which :c:macro:`ARROW_ASSIGN_OR_RAISE` 
can process. If the method fails, it will return to ``main()`` with a :class:`Status`
that will explain what went wrong. If it succeeds, then it will assign
the final output to the left-hand variable.

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: int8builder 1 Finish)
  :end-before: (Doc section: int8builder 1 Finish)

As soon as :class:`ArrayBuilder` has had its :func:`Finish <ArrayBuilder::Finish>` method called, its state resets, so
it can be used again, as if it was fresh. Thus, we repeat the process above for our second array:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: int8builder 2)
  :end-before: (Doc section: int8builder 2)

Building int16 Arrays
^^^^^^^^^^^^^^^^^^^^^

An :class:`ArrayBuilder` has its type specified at the time of declaration.
Once this is done, it cannot have its type changed. We have to make a new one when we switch to year data, which
requires a 16-bit integer at the minimum. Of course, there’s an :class:`ArrayBuilder` for that. 
It uses the exact same methods, but with the new data type:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: int16builder)
  :end-before: (Doc section: int16builder)

Now, we have three Arrow :class:`Arrays <arrow::Array>`, with some variance in type.

Making a RecordBatch
--------------------

A columnar data format only really comes into play when you have a table. 
So, let’s make one. The first kind we’ll make is the :class:`RecordBatch` – this 
uses :class:`Arrays <Array>` internally, which means all data will be contiguous within each 
column, but any appending or concatenating will require copying. Making a :class:`RecordBatch`
has two steps, given existing :class:`Arrays <Array>`:

#. Defining a :class:`Schema`
#. Loading the :class:`Schema` and Arrays into the constructor

Defining a Schema 
^^^^^^^^^^^^^^^^^

To get started making a :class:`RecordBatch`, we first need to define
characteristics of the columns, each represented by a :class:`Field` instance.
Each :class:`Field` contains a name and datatype for its associated column; then,
a :class:`Schema` groups them together and sets the order of the columns, like
so:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: Schema)
  :end-before: (Doc section: Schema)

Building a RecordBatch
^^^^^^^^^^^^^^^^^^^^^^

With data in :class:`Arrays <Array>` from the previous section, and column descriptions in our 
:class:`Schema` from the previous step, we can make the :class:`RecordBatch`. Note that the 
length of the columns is necessary, and the length is shared by all columns.

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: RBatch)
  :end-before: (Doc section: RBatch)

Now, we have our data in a nice tabular form, safely within the :class:`RecordBatch`.
What we can do with this will be discussed in the later tutorials. 

Making a ChunkedArray
---------------------

Let’s say that we want an array made up of sub-arrays, because it
can be useful for avoiding data copies when concatenating, for parallelizing work, for fitting each chunk
cutely into cache, or for exceeding the 2,147,483,647 row limit in a
standard Arrow :class:`Array`. For this, Arrow offers :class:`ChunkedArray`, which can be
made up of individual Arrow :class:`Arrays <Array>`. In this example, we can reuse the arrays
we made earlier in part of our chunked array, allowing us to extend them without having to copy
data. So, let’s build a few more :class:`Arrays <Array>`,
using the same builders for ease of use:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: More Arrays)
  :end-before: (Doc section: More Arrays)

In order to support an arbitrary amount of :class:`Arrays <Array>` in the construction of the 
:class:`ChunkedArray`, Arrow supplies :class:`ArrayVector`. This provides a vector for :class:`Arrays <Array>`,
and we'll use it here to prepare to make a :class:`ChunkedArray`:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: ArrayVector)
  :end-before: (Doc section: ArrayVector)

In order to leverage Arrow, we do need to take that last step, and move into a :class:`ChunkedArray`:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: ChunkedArray Day)
  :end-before: (Doc section: ChunkedArray Day)

With a :class:`ChunkedArray` for our day values, we now just need to repeat the process
for the month and year data:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: ChunkedArray Month Year)
  :end-before: (Doc section: ChunkedArray Month Year)

With that, we are left with three :class:`ChunkedArrays <ChunkedArray>`, varying in type. 

Making a Table
--------------

One particularly useful thing we can do with the :class:`ChunkedArrays <ChunkedArray>` from the previous section is creating 
:class:`Tables <Table>`. Much like a :class:`RecordBatch`, a :class:`Table` stores tabular data. However, a 
:class:`Table` does not guarantee contiguity, due to being made up of :class:`ChunkedArrays <ChunkedArray>`.
This can be useful for logic, paralellizing work, for fitting chunks into cache, or exceeding the 2,147,483,647 row limit
present in :class:`Array` and, thus, :class:`RecordBatch`.

If you read up to :class:`RecordBatch`, you may note that the :class:`Table` constructor in the following code is  
effectively identical, it just happens to put the length of the columns
in position 3, and makes a :class:`Table`. We re-use the :class:`Schema` from before, and
make our :class:`Table`:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: Table)
  :end-before: (Doc section: Table)

Now, we have our data in a nice tabular form, safely within the :class:`Table`.
What we can do with this will be discussed in the later tutorials. 

Ending Program 
--------------

At the end, we just return :func:`Status::OK()`, so the ``main()`` knows that
we’re done, and that everything’s okay.

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: Ret)
  :end-before: (Doc section: Ret)

Wrapping Up 
-----------

With that, you’ve created the fundamental data structures in Arrow, and
can proceed to getting them in and out of a program with file I/O in the next article.

Refer to the below for a copy of the complete code:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/arrow_example.cc
  :language: cpp
  :start-after: (Doc section: Basic Example)
  :end-before: (Doc section: Basic Example)
  :linenos:
  :lineno-match:
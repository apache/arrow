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

==============
Arrow File I/O
==============

Apache Arrow provides file I/O functions to facilitate use of Arrow from
the start to end of an application. In this article, you will:

1. Read an Arrow file into a :class:`RecordBatch` and write it back out afterwards

2. Read a CSV file into a :class:`Table` and write it back out afterwards

3. Read a Parquet file into a :class:`Table` and write it back out afterwards

Pre-requisites 
---------------

Before continuing, make sure you have:

1. An Arrow installation, which you can set up here: :doc:`/cpp/build_system`

2. An understanding of basic Arrow data structures from :doc:`/cpp/tutorials/basic_arrow`

3. A directory to run the final application in – this program will generate some files, so be prepared for that.

Setup
-----

Before writing out some file I/O, we need to fill in a couple gaps:

1. We need to include necessary headers.
   
2. A ``main()`` is needed to glue things together.

3. We need files to play with.

Includes
^^^^^^^^

Before writing C++ code, we need some includes. We'll get ``iostream`` for output, then import Arrow's 
I/O functionality for each file type we'll work with in this article: 

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Includes)
  :end-before: (Doc section: Includes)

Main()
^^^^^^

For our glue, we’ll use the ``main()`` pattern from the previous tutorial on
data structures:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Main)
  :end-before: (Doc section: Main)

Which, like when we used it before, is paired with a ``RunMain()``:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: RunMain)
  :end-before: (Doc section: RunMain)

Generating Files for Reading
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We need some files to actually play with. In practice, you’ll likely
have some input for your own application. Here, however, we want to
explore doing I/O for the sake of it, so let’s generate some files to make
this easy to follow. To create those, we’ll define a helper function
that we’ll run first. Feel free to read through this, but the concepts
used will be explained later in this article. Note that we’re using the
day/month/year data from the previous tutorial. For now, just copy the
function in:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: GenInitialFile)
  :end-before: (Doc section: GenInitialFile)

To get the files for the rest of your code to function, make sure to
call ``GenInitialFile()`` as the very first line in ``RunMain()`` to initialize
the environment:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Gen Files)
  :end-before: (Doc section: Gen Files)

I/O with Arrow Files
--------------------

We’re going to go through this step by step, reading then writing, as
follows:

1. Reading a file

   a. Open the file

   b. Bind file to :class:`ipc::RecordBatchFileReader`

   c. Read file to :class:`RecordBatch`

2. Writing a file

   a. Get a :class:`io::FileOutputStream`

   b. Write to file from :class:`RecordBatch`

Opening a File
^^^^^^^^^^^^^^

To actually read a file, we need to get some sort of way to point to it.
In Arrow, that means we’re going to get a :class:`io::ReadableFile` object – much
like an :class:`ArrayBuilder` can clear and make new arrays, we can reassign this
to new files, so we’ll use this instance throughout the examples:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: ReadableFile Definition)
  :end-before: (Doc section: ReadableFile Definition)

A :class:`io::ReadableFile` does little alone – we actually have it bind to a file
with :func:`io::ReadableFile::Open`. For
our purposes here, the default arguments suffice:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Arrow ReadableFile Open)
  :end-before: (Doc section: Arrow ReadableFile Open)

Opening an Arrow file Reader
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An :class:`io::ReadableFile` is too generic to offer all functionality to read an Arrow file.
We need to use it to get an :class:`ipc::RecordBatchFileReader` object. This object implements 
all the logic needed to read an Arrow file with correct formatting. We get one through 
:func:`ipc::RecordBatchFileReader::Open`:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Arrow Read Open)
  :end-before: (Doc section: Arrow Read Open)

Reading an Open Arrow File to RecordBatch
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We have to use a :class:`RecordBatch` to read an Arrow file, so we’ll get a
:class:`RecordBatch`. Once we have that, we can actually read the file. Arrow
files can have multiple :class:`RecordBatches <RecordBatch>`, so we must pass an index. This
file only has one, so pass 0:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Arrow Read)
  :end-before: (Doc section: Arrow Read)

Prepare a FileOutputStream
^^^^^^^^^^^^^^^^^^^^^^^^^^

For output, we need a :class:`io::FileOutputStream`. Just like our :class:`io::ReadableFile`,
we’ll be reusing this, so be ready for that. We open files the same way
as when reading:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Arrow Write Open)
  :end-before: (Doc section: Arrow Write Open)

Write Arrow File from RecordBatch
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now, we grab our :class:`RecordBatch` we read into previously, and use it, along
with our target file, to create a :class:`ipc::RecordBatchWriter`. The
:class:`ipc::RecordBatchWriter` needs two things:

1. the target file

2. the :class:`Schema` for our :class:`RecordBatch` (in case we need to write more :class:`RecordBatches <RecordBatch>` of the same format.)

The :class:`Schema` comes from our existing :class:`RecordBatch` and the target file is
the output stream we just created.

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Arrow Writer)
  :end-before: (Doc section: Arrow Writer)

We can just call :func:`ipc::RecordBatchWriter::WriteRecordBatch` with our :class:`RecordBatch` to fill up our
file:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Arrow Write)
  :end-before: (Doc section: Arrow Write)

For IPC in particular, the writer has to be closed since it anticipates more than one batch may be written. To do that:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Arrow Close)
  :end-before: (Doc section: Arrow Close)

Now we’ve read and written an IPC file!

I/O with CSV
------------

We’re going to go through this step by step, reading then writing, as
follows:

1. Reading a file

   a. Open the file

   b. Prepare Table

   c. Read File using :class:`csv::TableReader`

2. Writing a file

   a. Get a :class:`io::FileOutputStream`

   b. Write to file from :class:`Table`

Opening a CSV File
^^^^^^^^^^^^^^^^^^

For a CSV file, we need to open a :class:`io::ReadableFile`, just like an Arrow file,
and reuse our :class:`io::ReadableFile` object from before to do so:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: CSV Read Open)
  :end-before: (Doc section: CSV Read Open)

Preparing a Table
^^^^^^^^^^^^^^^^^

CSV can be read into a :class:`Table`, so declare a pointer to a :class:`Table`:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: CSV Table Declare)
  :end-before: (Doc section: CSV Table Declare)

Read a CSV File to Table
^^^^^^^^^^^^^^^^^^^^^^^^

The CSV reader has option structs which need to be passed – luckily,
there are defaults for these which we can pass directly. For reference
on the other options, go here: :doc:`/cpp/api/formats`.
without any special delimiters and is small, so we can make our reader
with defaults:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: CSV Reader Make)
  :end-before: (Doc section: CSV Reader Make)

With the CSV reader primed, we can use its :func:`csv::TableReader::Read` method to fill our
:class:`Table`:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: CSV Read)
  :end-before: (Doc section: CSV Read)

Write a CSV File from Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^

CSV writing to :class:`Table` looks exactly like IPC writing to :class:`RecordBatch`,
except with our :class:`Table`, and using :func:`ipc::RecordBatchWriter::WriteTable` instead of
:func:`ipc::RecordBatchWriter::WriteRecordBatch`. Note that the same writer class is used -- 
we're writing with :func:`ipc::RecordBatchWriter::WriteTable` because we have a :class:`Table`. We’ll target 
a file, use our :class:`Table’s <Table>` :class:`Schema`, and then write the :class:`Table`:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: CSV Write)
  :end-before: (Doc section: CSV Write)

Now, we’ve read and written a CSV file!

File I/O with Parquet
---------------------

We’re going to go through this step by step, reading then writing, as
follows:

1. Reading a file

   a. Open the file

   b. Prepare :class:`parquet::arrow::FileReader`

   c. Read file to :class:`Table`

2. Writing a file

   a. Write :class:`Table` to file

Opening a Parquet File
^^^^^^^^^^^^^^^^^^^^^^

Once more, this file format, Parquet, needs a :class:`io::ReadableFile`, which we
already have, and for the :func:`io::ReadableFile::Open` method to be called on a file:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Parquet Read Open)
  :end-before: (Doc section: Parquet Read Open)

Setting up a Parquet Reader
^^^^^^^^^^^^^^^^^^^^^^^^^^^

As always, we need a Reader to actually read the file. We’ve been
getting Readers for each file format from the Arrow namespace. This
time, we enter the Parquet namespace to get the :class:`parquet::arrow::FileReader`:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Parquet FileReader)
  :end-before: (Doc section: Parquet FileReader)

Now, to set up our reader, we call :func:`parquet::arrow::OpenFile`. Yes, this is necessary
even though we used :func:`io::ReadableFile::Open`. Note that we pass our
:class:`parquet::arrow::FileReader` by reference, instead of assigning to it in output:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Parquet OpenFile)
  :end-before: (Doc section: Parquet OpenFile)

Reading a Parquet File to Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With a prepared :class:`parquet::arrow::FileReader` in hand, we can read to a 
:class:`Table`, except we must pass the :class:`Table` by reference instead of outputting to it:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Parquet Read)
  :end-before: (Doc section: Parquet Read)

Writing a Parquet File from Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For single-shot writes, writing a Parquet file does not need a writer object. Instead, we give
it our table, point to the memory pool it will use for any necessary
memory consumption, tell it where to write, and the chunk size if it
needs to break up the file at all:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Parquet Write)
  :end-before: (Doc section: Parquet Write)

Ending Program
--------------

At the end, we just return :func:`Status::OK`, so the ``main()`` knows that
we’re done, and that everything’s okay. Just like in the first tutorial.

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: Return)
  :end-before: (Doc section: Return)

With that, you’ve read and written IPC, CSV, and Parquet in Arrow, and
can properly load data and write output! Now, we can move into
processing data with compute functions in the next article.

Refer to the below for a copy of the complete code:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/file_access_example.cc
  :language: cpp
  :start-after: (Doc section: File I/O)
  :end-before: (Doc section: File I/O)
  :linenos:
  :lineno-match:
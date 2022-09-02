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

1. Read an Arrow file into a RecordBatch and write it back out afterwards

2. Read a CSV file into a Table and write it back out afterwards

3. Read a Parquet file into a Table and write it back out afterwards

Pre-requisites 
---------------

Before continuing, make sure you have:

1. An Arrow installation

2. An understanding of basic Arrow data structures from <the preceding article>

3. A directory to run the final application in – this program will generate some files, so be prepared for that.

Setup
-----

Before writing out some file I/O, we need to fill in a couple gaps:

1. A main() is needed to glue things together.

2. We need files to actually play with.

Main()
^^^^^^

For our glue, we’ll use the main() pattern from the previous tutorial on
data structures:

<MAIN WITH STATUS CHECKER>

Which, like when we used it before, is paired with a RunMain():

<RUNMAIN START>

Generating Files for Reading
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We need some files to actually play with. In practice, you’ll likely
have some input for your own application. Here, however, we want to
explore doing I/O for the sake of it, so let’s generate some to make
this easy to follow. To create those, we’ll define a helper function
that we’ll run first. Feel free to read through this, but the concepts
used will be explained later in this article. Note that we’re using the
day/month/year data from the previous tutorial. For now, just copy the
function in:

<GENINITIALFILE>

To get the files for the rest of your code to function, make sure to
call GenInitialFile() as the very first line in RunMain() to initialize
the environment:

<CALL GENINITIALFILE>

I/O with Arrow Files
--------------------

We’re going to go through this step by step, reading then writing, as
follows:

1. Reading a file

   a. Open the file

   b. Read file to RecordBatch

2. Writing a file

   a. Get a FileOutputStream

   b. Write to file from RecordBatch

Opening a File
^^^^^^^^^^^^^^

To actually read a file, we need to get some sort of way to point to it.
In Arrow, that means we’re going to get a ReadableFile object – much
like an ArrayBuilder can clear and make new arrays, we can reassign this
to new files, so we’ll use this instance throughout the examples:

<READABLEFILE DEFINITION>

A ReadbleFile does little alone – we actually have it bind to a file
with Open(). You can read more on that and its arguments at <link>. For
our purposes here, the default arguments suffice:

<OPEN USAGE>

Reading an Open Arrow File to RecordBatch
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We have to use a RecordBatch to read an Arrow file, so we’ll get a
RecordBatch. Once we have that, we can actually read the file. Arrow
files can have multiple RecordBatches, so we must pass an index. This
file only has one, so pass 0:

<READ INTO RECORDBATCH>

Prepare a FileOutputStream
^^^^^^^^^^^^^^^^^^^^^^^^^^

For output, we need a FileOutputStream. Just like our ReadableFile,
we’ll be reusing this, so be ready for that. We open files the same way
as when reading:

<DEFINE FILEOUTPUTSTREAM AND OPEN IPC FILE>

Write Arrow File from RecordBatch
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now, we grab our RecordBatch we read into previously, and use it, along
with our target file, to create a RecordBatchWriter. The
RecordBatchWriter needs two things:

1. the target file

2. the Schema for our RecordBatch (in case we need to write more RecordBatches of the same format.)

The Schema comes from our existing RecordBatch, and the target file is
just a name – in this case, test_out.arrow.

<MAKE RECORDBATCH WRITER>

We can just call WriteRecordBatch() with our RecordBatch to fill up our
file:

<WRITE BATCH>

For IPC in particular, the writer has to be closed, so do that:

<CLOSE IPC WRITER>

Now we’ve read and written an IPC file!

I/O with CSV
------------

We’re going to go through this step by step, reading then writing, as
follows:

1. Reading a file

   a. Open the file

   b. Prepare Table

   c. Read File using CSV Reader

2. Writing a file

   a. Get a FileOutputStream

   b. Write to file from Table

Opening a CSV File
^^^^^^^^^^^^^^^^^^

For a CSV file, we need to open a ReadableFile, just like an Arrow file,
and reuse our ReadableFile object from before to do so:

<OPEN CSV>

Preparing a Table
^^^^^^^^^^^^^^^^^

CSV can be read into a Table, so declare a pointer to a Table:

<DECLARE TABLE POINTER>

Read a CSV File to Table
^^^^^^^^^^^^^^^^^^^^^^^^

The CSV reader has option structs which need to be passed – luckily,
there are defaults for these which we can pass directly. For reference
on the other options, go here: <link>. This CSV is a standard CSV
without any special delimiters and is small, so we can make our reader
with defaults:

<MAKE CSV READER>

With the CSV reader primed, we can use its Read() method to fill our
Table:

<READ CSV>

Write a CSV File from Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^

| CSV writing to Table looks exactly like IPC writing to RecordBatch,
  except with our Table, and using WriteTable() instead of
  WriteRecordBatch(). We’ll target a file, use our Table’s Schema, and
  then write the Table:
| <ENTIRE CSV WRITE PROCESS>

Now, we’ve read and written a CSV file!

File I/O with Parquet
---------------------

We’re going to go through this step by step, reading then writing, as
follows:

1. Reading a file

   a. Open the file

   b. Prepare Reader

   c. Read file to Table

2. Writing a file

   a. Write table to file

Opening a Parquet File
^^^^^^^^^^^^^^^^^^^^^^

Once more, this file format, Parquet, needs a ReadableFile, which we
already have, and for the Open() method to be called on a file:

<OPEN PARQUET FILE>

Setting up a Parquet Reader
^^^^^^^^^^^^^^^^^^^^^^^^^^^

As always, we need a Reader to actually read the file. We’ve been
getting Readers for each file format from the Arrow namespace. This
time, we enter the Parquet namespace to get the Reader:

<GET FILEREADER>

Now, to set up our reader, we call OpenFile(). Yes, this is necessary
even though we used ReadableFile::Open(). Note that we pass our
FileReader by reference, instead of assigning to it in output:

<PARQUET OPENFILE>

Reading a Parquet File to Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With a prepared FileReader in hand, we can read to a Table, except we
must pass the Table by reference instead of outputting to it:

<DECLARE TABLE AND READ INTO IT>

Writing a Parquet File from Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Writing a Parquet file does not need a writer object. Instead, we give
it our table, point to the memory pool it will use for any necessary
memory consumption, tell it where to write, and the chunk size if it
needs to break up the file at all:

<WRITE TABLE TO PARQUET>

Ending Program
--------------

At the end, we just return arrow::Status::OK(), so the main() knows that
we’re done, and that everything’s okay. Just like in the first tutorial.

<RETURN STATEMENT>

With that, you’ve read and written IPC, CSV, and Parquet in Arrow, and
can properly load data and write output! Now, we can move into
processing data with compute functions.

<LINK TO NEXT ARTICLE>

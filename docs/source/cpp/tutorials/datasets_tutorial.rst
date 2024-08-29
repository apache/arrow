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
Arrow Datasets
==============

Arrow C++ provides the concept and implementation of :class:`Datasets <dataset::Dataset>` to work
with fragmented data, which can be larger-than-memory, be that due to
generating large amounts, reading in from a stream, or having a large
file on disk. In this article, you will:

1. read a multi-file partitioned dataset and put it into a Table,

2. write out a partitioned dataset from a Table.

Pre-requisites 
---------------

Before continuing, make sure you have:

1. An Arrow installation, which you can set up here: :doc:`/cpp/build_system`

2. An understanding of basic Arrow data structures from :doc:`/cpp/tutorials/basic_arrow`

To witness the differences, it may be useful to have also read the :doc:`/cpp/tutorials/io_tutorial`. However, it is not required.

Setup
-----

Before running some computations, we need to fill in a couple gaps:

1. We need to include necessary headers.
   
2. A ``main()`` is needed to glue things together.

3. We need data on disk to play with.

Includes
^^^^^^^^

Before writing C++ code, we need some includes. We'll get ``iostream`` for output, then import Arrow's 
compute functionality for each file type we'll work with in this article: 

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Includes)
  :end-before: (Doc section: Includes)

Main()
^^^^^^

For our glue, we’ll use the ``main()`` pattern from the previous tutorial on
data structures:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Main)
  :end-before: (Doc section: Main)

Which, like when we used it before, is paired with a ``RunMain()``:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: RunMain)
  :end-before: (Doc section: RunMain)

Generating Files for Reading
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We need some files to actually play with. In practice, you’ll likely
have some input for your own application. Here, however, we want to
explore without the overhead of supplying or finding a dataset, so let’s
generate some to make this easy to follow. Feel free to read through
this, but the concepts will be visited properly in this article – just
copy it in, for now, and realize it ends with a partitioned dataset on
disk:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Helper Functions)
  :end-before: (Doc section: Helper Functions)

In order to actually have these files, make sure the first thing called
in ``RunMain()`` is our helper function ``PrepareEnv()``, which will get a
dataset on disk for us to play with:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: PrepareEnv)
  :end-before: (Doc section: PrepareEnv)

Reading a Partitioned Dataset
-----------------------------

Reading a Dataset is a distinct task from reading a single file. The
task takes more work than reading a single file, due to needing to be
able to parse multiple files and/or folders. This process can be broken
up into the following steps:

1. Getting a :class:`fs::FileSystem` object for the local FS

2. Create a :class:`fs::FileSelector` and use it to prepare a :class:`dataset::FileSystemDatasetFactory`

3. Build a :class:`dataset::Dataset` using the :class:`dataset::FileSystemDatasetFactory`

4. Use a :class:`dataset::Scanner` to read into a :class:`Table`

Preparing a FileSystem Object
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to begin, we’ll need to be able to interact with the local
filesystem. In order to do that, we’ll need an :class:`fs::FileSystem` object.
A :class:`fs::FileSystem` is an abstraction that lets us use the same interface
regardless of using Amazon S3, Google Cloud Storage, or local disk – and
we’ll be using local disk. So, let’s declare it:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: FileSystem Declare)
  :end-before: (Doc section: FileSystem Declare)

For this example, we’ll have our :class:`FileSystem’s <fs::FileSystem>` base path exist in the
same directory as the executable. :func:`fs::FileSystemFromUriOrPath` lets us get
a :class:`fs::FileSystem` object for any of the types of supported filesystems.
Here, though, we’ll just pass our path:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: FileSystem Init)
  :end-before: (Doc section: FileSystem Init)

.. seealso:: :class:`fs::FileSystem` for the other supported filesystems.

Creating a FileSystemDatasetFactory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A :class:`fs::FileSystem` stores a lot of metadata, but we need to be able to
traverse it and parse that metadata. In Arrow, we use a :class:`FileSelector` to
do so:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: FileSelector Declare)
  :end-before: (Doc section: FileSelector Declare)

This :class:`fs::FileSelector` isn’t able to do anything yet. In order to use it, we
need to configure it – we’ll have it start any selection in
“parquet_dataset,” which is where the environment preparation process
has left us a dataset, and set recursive to true, which allows for
traversal of folders.

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: FileSelector Config)
  :end-before: (Doc section: FileSelector Config)

To get a :class:`dataset::Dataset` from a :class:`fs::FileSystem`, we need to prepare a
:class:`dataset::FileSystemDatasetFactory`. This is a long but descriptive name – it’ll
make us a factory to get data from our :class:`fs::FileSystem`. First, we configure
it by filling a :class:`dataset::FileSystemFactoryOptions` struct:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: FileSystemFactoryOptions)
  :end-before: (Doc section: FileSystemFactoryOptions)

There are many file formats, and we have to pick one that will be
expected when actually reading. Parquet is what we have on disk, so of
course we’ll ask for that when reading:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: File Format Setup)
  :end-before: (Doc section: File Format Setup)

After setting up the :class:`fs::FileSystem`, :class:`fs::FileSelector`, options, and file format,
we can make that :class:`dataset::FileSystemDatasetFactory`. This simply requires passing
in everything we’ve prepared and assigning that to a variable:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: FileSystemDatasetFactory Make)
  :end-before: (Doc section: FileSystemDatasetFactory Make)

Build Dataset using Factory
^^^^^^^^^^^^^^^^^^^^^^^^^^^

With a :class:`dataset::FileSystemDatasetFactory` set up, we can actually build our
:class:`dataset::Dataset` with :func:`dataset::FileSystemDatasetFactory::Finish`, just 
like with an :class:`ArrayBuilder` back in the basic tutorial:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: FileSystemDatasetFactory Finish)
  :end-before: (Doc section: FileSystemDatasetFactory Finish)

Now, we have a :class:`dataset::Dataset` object in memory. This does not mean that the
entire dataset is manifested in memory, but that we now have access to
tools that allow us to explore and use the dataset that is on disk. For
example, we can grab the fragments (files) that make up our whole
dataset, and print those out, along with some small info:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Dataset Fragments)
  :end-before: (Doc section: Dataset Fragments)

Move Dataset into Table
^^^^^^^^^^^^^^^^^^^^^^^

One way we can do something with :class:`Datasets <dataset::Dataset>` is getting 
them into a :class:`Table`, where we can do anything we’ve learned we can do to 
:class:`Tables <Table>` to that :class:`Table`. 

.. seealso:: :doc:`/cpp/streaming_execution` for execution that avoids manifesting the entire dataset in memory.

In order to move a :class:`Dataset’s <dataset::Dataset>` contents into a :class:`Table`, 
we need a :class:`dataset::Scanner`, which scans the data and outputs it to the :class:`Table`. 
First, we get a :class:`dataset::ScannerBuilder` from the :class:`dataset::Dataset`:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Read Scan Builder)
  :end-before: (Doc section: Read Scan Builder)

Of course, a Builder’s only use is to get us our :class:`dataset::Scanner`, so let’s use
:func:`dataset::ScannerBuilder::Finish`:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Read Scanner)
  :end-before: (Doc section: Read Scanner)

Now that we have a tool to move through our :class:`dataset::Dataset`, let’s use it to get
our :class:`Table`. :func:`dataset::Scanner::ToTable` offers exactly what we’re looking for,
and we can print the results:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: To Table)
  :end-before: (Doc section: To Table)

This leaves us with a normal :class:`Table`. Again, to do things with :class:`Datasets <dataset::Dataset>`
without moving to a :class:`Table`, consider using Acero.

Writing a Dataset to Disk from Table
------------------------------------

Writing a :class:`dataset::Dataset` is a distinct task from writing a single file. The
task takes more work than writing a single file, due to needing to be
able to parse handle a partitioning scheme across multiple files and
folders. This process can be broken up into the following steps:

1. Prepare a :class:`TableBatchReader`

2. Create a :class:`dataset::Scanner` to pull data from :class:`TableBatchReader`

3. Prepare schema, partitioning, and file format options

4. Set up :class:`dataset::FileSystemDatasetWriteOptions` – a struct that configures our writing functions

5. Write dataset to disk

Prepare Data from Table for Writing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We have a :class:`Table`, and we want to get a :class:`dataset::Dataset` on disk. In fact, for the
sake of exploration, we’ll use a different partitioning scheme for the
dataset – instead of just breaking into halves like the original
fragments, we’ll partition based on each row’s value in the “a” column.

To get started on that, let’s get a :class:`TableBatchReader`! This makes it very
easy to write to a :class:`Dataset`, and can be used elsewhere whenever a :class:`Table`
needs to be broken into a stream of :class:`RecordBatches <RecordBatch>`. Here, we can just use
the :class:`TableBatchReader’s <TableBatchReader>` constructor, with our table:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: TableBatchReader)
  :end-before: (Doc section: TableBatchReader)

Create Scanner for Moving Table Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The process for writing a :class:`dataset::Dataset`, once a source of data is available,
is similar to the reverse of reading it. Before, we used a :class:`dataset::Scanner` in
order to scan into a :class:`Table` – now, we need one to read out of our
:class:`TableBatchReader`. To get that :class:`dataset::Scanner`, we’ll make a :class:`dataset::ScannerBuilder` 
based on our :class:`TableBatchReader`, then use that Builder to build a :class:`dataset::Scanner`:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: WriteScanner)
  :end-before: (Doc section: WriteScanner)

Prepare Schema, Partitioning, and File Format Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since we want to partition based on the “a” column, we need to declare
that. When defining our partitioning :class:`Schema`, we’ll just have a single
:class:`Field` that contains “a”:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Partition Schema)
  :end-before: (Doc section: Partition Schema)

This :class:`Schema` determines what the key is for partitioning, but we need to
choose the algorithm that’ll do something with this key. We will use
Hive-style again, this time with our schema passed to it as
configuration:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Partition Create)
  :end-before: (Doc section: Partition Create)

Several file formats are available, but Parquet is commonly used with
Arrow, so we’ll write back out to that:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Write Format)
  :end-before: (Doc section: Write Format)

Configure FileSystemDatasetWriteOptions 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to write to disk, we need some configuration. We’ll do so via
setting values in a :class:`dataset::FileSystemDatasetWriteOptions` struct. We’ll
initialize it with defaults where possible:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Write Options)
  :end-before: (Doc section: Write Options)

One important step in writing to file is having a :class:`fs::FileSystem` to target.
Luckily, we have one from when we set it up for reading. This is a
simple variable assignment:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Options FS)
  :end-before: (Doc section: Options FS)

Arrow can make the directory, but it does need a name for said
directory, so let’s give it one, call it “write_dataset”:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Options Target)
  :end-before: (Doc section: Options Target)

We made a partitioning method previously, declaring that we’d use
Hive-style – this is where we actually pass that to our writing
function:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Options Partitioning)
  :end-before: (Doc section: Options Partitioning)

Part of what’ll happen is Arrow will break up files, thus preventing
them from being too large to handle. This is what makes a dataset
fragmented in the first place. In order to set this up, we need a base
name for each fragment in a directory – in this case, we’ll have
“part{i}.parquet”, which means the third file (within the same
directory) will be called “part3.parquet”, for example:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Options Name Template)
  :end-before: (Doc section: Options Name Template)

Sometimes, data will be written to the same location more than once, and
overwriting will be accepted. Since we may want to run this application
more than once, we will set Arrow to overwrite existing data – if we
didn’t, Arrow would abort due to seeing existing data after the first
run of this application:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Options File Behavior)
  :end-before: (Doc section: Options File Behavior)

Write Dataset to Disk
^^^^^^^^^^^^^^^^^^^^^

Once the :class:`dataset::FileSystemDatasetWriteOptions` has been configured, and a
:class:`dataset::Scanner` is prepared to parse the data, we can pass the Options and
:class:`dataset::Scanner` to the :func:`dataset::FileSystemDataset::Write` to write out to
disk:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Write Dataset)
  :end-before: (Doc section: Write Dataset)

You can review your disk to see that you’ve written a folder containing
subfolders for every value of “a”, which each have Parquet files!

Ending Program
--------------

At the end, we just return :func:`Status::OK`, so the ``main()`` knows that
we’re done, and that everything’s okay, just like the preceding
tutorials.

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Ret)
  :end-before: (Doc section: Ret)

With that, you’ve read and written partitioned datasets! This method,
with some configuration, will work for any supported dataset format. For
an example of such a dataset, the NYC Taxi dataset is a well-known
one, which you can find `here <https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page>`_. 
Now you can get larger-than-memory data mapped for use!

Which means that now we have to be able to process this data without
pulling it all into memory at once. For this, try Acero. 

.. seealso:: :doc:`/cpp/streaming_execution` for more information on Acero.

Refer to the below for a copy of the complete code:

.. literalinclude:: ../../../../cpp/examples/tutorial_examples/dataset_example.cc
  :language: cpp
  :start-after: (Doc section: Dataset Example)
  :end-before: (Doc section: Dataset Example)
  :linenos:
  :lineno-match:
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

Arrow C++ provides the concept and implementation of Datasets to work
with fragmented data, which can be larger-than-memory, be that due to
generating large amounts, reading in from a stream, or having a large
file on disk. In this article, you will:

1. read a multi-file partitioned dataset and put it into a Table,

2. write out a partitioned dataset from a Table.

Pre-requisites 
---------------

Before continuing, make sure you have:

1. An Arrow installation

2. An understanding of basic Arrow data structures from <basic data structures>

To witness the differences, it may be useful to have also read the <File
I/O tutorial>. However, it is not required.

Setup
-----

Before running some computations, we need to fill in a couple gaps:

1. A main() is needed to glue things together.

2. We need data on disk to play with.

Main()
~~~~~~

For our glue, we’ll use the main() pattern from the previous tutorial on
data structures:

<MAIN WITH STATUS CHECKER>

Which, like when we used it before, is paired with a RunMain():

<RUNMAIN START>

Generating Files for Reading
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We need some files to actually play with. In practice, you’ll likely
have some input for your own application. Here, however, we want to
explore without the overhead of supplying or finding a dataset, so let’s
generate some to make this easy to follow. Feel free to read through
this, but the concepts will be visited properly in this article – just
copy it in, for now, and realize it ends with a partitioned dataset on
disk:

<HELPER FUNCTIONS>

In order to actually have these files, make sure the first thing called
in RunMain() isl our helper function PrepareEnv(), which will get a
dataset on disk for us to play with:

<CALL PREPAREENV()>

Reading a Partitioned Dataset
-----------------------------

Reading a Dataset is a distinct task from reading a single file. The
task takes more work than reading a single file, due to needing to be
able to parse multiple files and/or folders. This process can be broken
up into the following steps:

1. Getting a FileSystem object for the local FS

2. Create a FileSelector and use it to prepare a FileSystemDatasetFactory

3. Build a Dataset using the FileSystemDatasetFactory

4. Use a Scanner to read into a Table

Preparing a FileSystem Object
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to begin, we’ll need to be able to interact with the local
filesystem. In order to do that, we’ll need an Arrow FileSystem object.
A FileSystem is an abstraction that lets us use the same interface
regardless of using Amazon S3, Google Cloud Storage, or local disk – and
we’ll be using local disk. So, let’s declare it:

<FILESYSTEM DECLARE>

For this example, we’ll have our FileSystem’s base path exist in the
same directory as the executable. FileSystemFromUriOrPath() lets us get
a FileSystem object for any of the types of supported filesystems; check
the API docs for more possibilities<link>. Here, though, we’ll just pass
our path:

<FILESYSTEM INITIALIZE>

Creating a FileSystemDatasetFactory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A FileSystem stores a lot of metadata, but we need to be able to
traverse it and parse that metadata. In Arrow, we use a FileSelector to
do so:

<FILESELECTOR DECLARE>

This FileSelector isn’t able to do anything yet. In order to use it, we
need to configure it – we’ll have it start any selection in
“parquet_dataset,” which is where the environment preparation process
has left us a dataset, and set recursive to true, which allows for
traversal of folders.

<FILESELECTOR CONFIGURE>

To get a Dataset from a Filesystem, we need to prepare a
FileSystemDatasetFactory. This is a long but descriptive name – it’ll
make us a factory to get data from our FileSystem. First, we configure
it by filling a FileSystemFactoryOptions struct:

<FILESYSTEMFACTORYOPTIONS SETTINGS> (this includes comments which refer
to the Hive-style partitioning)

There are many file formats, and we have to pick one that will be
expected when actually reading. Parquet is what we have on disk, so of
course we’ll ask for that when reading:

<FILEFORMAT SETUP>

After setting up the FileSystem, FileSelector, options, and file format,
we can make that FileSystemDatasetFactory. This simply requires passing
in everything we’ve prepared and assigning that to a variable:

<FACTORY MAKE>

Build Dataset using Factory
~~~~~~~~~~~~~~~~~~~~~~~~~~~

With a FileSystemDatasetFactory set up, we can actually build our
Dataset with Finish(), just like with an ArrayBuilder back in the basic
tutorial:

<FACTORY FINISH>

Now, we have a Dataset object in memory. This does not mean that the
entire dataset is manifested in memory, but that we now have access to
tools that allow us to explore and use the dataset that is on disk. For
example, we can grab the fragments (files) that make up our whole
dataset, and print those out, along with some small info:

<DATASET FRAGMENT GET AND PRINT>

Move Dataset into Table
~~~~~~~~~~~~~~~~~~~~~~~

One way we can do something with Datasets is getting them into a Table,
where we can do anything we’ve learned we can do to Tables to that
Table. (For further functionality and avoiding manifesting entire
datasets in memory, Acero is best: <Link here>)

In order to move a Dataset’s contents into a Table, we need a Scanner,
which scans the data and outputs it to the Table. First, we get a
Scanner Builder from the Dataset:

<READ_SCAN_BUILDER>

Of course, a Builder’s only use is to get us our Scanner, so let’s use
Finish():

<READ_SCANNER>

Now that we have a tool to move through our Dataset, let’s use it to get
our Table. Scanner’s ToTable() offers exactly what we’re looking for,
and we can print the results:

<TOTABLE>

This leaves us with a normal Table. Again, to do things with Datasets
without moving to a Table, consider using Acero.

Writing a Dataset to Disk from Table
------------------------------------

Writing a Dataset is a distinct task from writing a single file. The
task takes more work than writing a single file, due to needing to be
able to parse handle a partitioning scheme across multiple files and
folders. This process can be broken up into the following steps:

1. Prepare a TableBatchReader

2. Create a Scanner to pull data from TableBatchReader

3. Prepare schema, partitioning, and file format options

4. Set up FileSystemDatasetWriteOptions – a struct that configures our
      writing functions

5. Write dataset to disk

Prepare Data from Table for Writing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We have a Table, and we want to get a Dataset on disk. In fact, for the
sake of exploration, we’ll use a different partitioning scheme for the
dataset – instead of just breaking into halves like the original
fragments, we’ll partition based on each row’s value in the “a” column.

To get started on that, let’s get a TableBatchReader! This makes it very
easy to write to a Dataset, and can be used elsewhere whenever a Table
needs to be broken into a stream of RecordBatches. Here, we can just use
the TableBatchReader’s constructor, with our table:

<TABLEBATCHREADER DECLARE+INIT>

Create Scanner for Moving Table Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The process for writing a Dataset, once a source of data is available,
is similar to the reverse of reading it. Before, we used a Scanner in
order to scan into a Table – now, we need one to read out of our
TableBatchReader. To get that Scanner, we’ll make a Builder based on our
TableBatchReader, then use that Builder to build a Scanner:

<WRITESCANNER CREATION>

Prepare Schema, Partitioning, and File Format Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since we want to partition based on the “a” column, we need to declare
that. When defining our partitioning Schema, we’ll just have a single
Field that contains “a”:

<SCHEMA CREATION>

This Schema determines what the key is for partitioning, but we need to
choose the algorithm that’ll do something with this key. We will use
Hive-style again, this time with our schema passed to it as
configuration:

<PARTITIONING CREATION>

Several file formats are available, but Parquet is commonly used with
Arrow, so we’ll write back out to that:

<PARQUETFILEFORMAT WRITE>

Configure FileSystemDatasetWriteOptions 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to write to disk, we need some configuration. We’ll do so via
setting values in a FileSystemDatasetWriteOptions struct. We’ll
initialize it with defaults where possible:

<DECLARE AND INITIALIZE OPTIONS>

One important step in writing to file is having a FileSystem to target.
Luckily, we have one from when we set it up for reading. This is a
simple variable assignment:

<ASSIGN FILESYSTEM>

Arrow can make the directory, but it does need a name for said
directory, so let’s give it one, call it “write_dataset”:

<ASSIGN TARGET>

We made a partitioning method previously, declaring that we’d use
Hive-style – this is where we actually pass that to our writing
function:

<ASSIGN PARTITIONING>

Part of what’ll happen is Arrow will break up files, thus preventing
them from being too large to handle. This is what makes a dataset
fragmented in the first place. In order to set this up, we need a base
name for each fragment in a directory – in this case, we’ll have
“part{i}.parquet”, which means the third file (within the same
directory) will be called “part3.parquet”, for example:

<ASSIGN NAME TEMPLATE>

Sometimes, data will be written to the same location more than once, and
overwriting will be accepted. Since we may want to run this application
more than once, we will set Arrow to overwrite existing data – if we
didn’t, Arrow would abort due to seeing existing data after the first
run of this application:

<ASSIGN EXISTING BEHAVIOR>

Write Dataset to Disk
~~~~~~~~~~~~~~~~~~~~~

Once the FileSystemDatasetWriteOptions has been configured, and a
Scanner is prepared to parse the data, we can pass the Options and
Scanner to the <Dataset write function, use rst here> to write out to
disk:

<WRITE DATASET>

You can review your disk to see that you’ve written a folder containing
subfolders for every value of “a”, which each have Parquet files!

Ending Program
--------------

At the end, we just return arrow::Status::OK(), so the main() knows that
we’re done, and that everything’s okay, just like the preceding
tutorials.

<RETURN STATEMENT>

With that, you’ve read and written partitioned datasets! This method,
with some configuration, will work for any supported dataset format. For
an example of such a dataset, the NYC Taxi dataset is a well-known
one<link>. Now you can get larger-than-memory data mapped for use!

Which means that now we have to be able to process this data without
pulling it all into memory at once. For this, we’ll use Acero.

<LINK TO NEXT ARTICLE>

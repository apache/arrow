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

.. _filesystem:

.. currentmodule:: pyarrow.fs

Filesystem Interface
====================

PyArrow comes with an abstract filesystem interface, as well as concrete
implementations for various storage types.

The filesystem interface provides input and output streams as well as
directory operations.  A simplified view of the underlying data
storage is exposed.  Data paths are represented as *abstract paths*, which
are ``/``-separated, even on Windows, and shouldn't include special path
components such as ``.`` and ``..``.  Symbolic links, if supported by the
underlying storage, are automatically dereferenced.  Only basic
:class:`metadata <FileInfo>` about file entries, such as the file size
and modification time, is made available.

The core interface is represented by the base class :class:`FileSystem`.

Pyarrow implements natively the following filesystem subclasses:

* :ref:`filesystem-localfs` (:class:`LocalFileSystem`)
* :ref:`filesystem-s3` (:class:`S3FileSystem`)
* :ref:`filesystem-gcs` (:class:`GcsFileSystem`)
* :ref:`filesystem-hdfs` (:class:`HadoopFileSystem`)

It is also possible to use your own fsspec-compliant filesystem with pyarrow functionalities as described in the section :ref:`filesystem-fsspec`.


.. _filesystem-usage:

Usage
-----

Instantiating a filesystem
~~~~~~~~~~~~~~~~~~~~~~~~~~

A FileSystem object can be created with one of the constructors (and check the
respective constructor for its options)::

   >>> from pyarrow import fs
   >>> local = fs.LocalFileSystem()

or alternatively inferred from a URI::

   >>> s3, path = fs.FileSystem.from_uri("s3://my-bucket")
   >>> s3
   <pyarrow._s3fs.S3FileSystem at 0x7f6760cbf4f0>
   >>> path
   'my-bucket'


Reading and writing files
~~~~~~~~~~~~~~~~~~~~~~~~~

Several of the IO-related functions in PyArrow accept either a URI (and infer
the filesystem) or an explicit ``filesystem`` argument to specify the filesystem
to read or write from. For example, the :meth:`pyarrow.parquet.read_table`
function can be used in the following ways::

   import pyarrow.parquet as pq

   # using a URI -> filesystem is inferred
   pq.read_table("s3://my-bucket/data.parquet")
   # using a path and filesystem
   s3 = fs.S3FileSystem(..)
   pq.read_table("my-bucket/data.parquet", filesystem=s3)

The filesystem interface further allows to open files for reading (input) or
writing (output) directly, which can be combined with functions that work with
file-like objects. For example::

   import pyarrow as pa

   local = fs.LocalFileSystem()

   with local.open_output_stream("test.arrow") as file:
      with pa.RecordBatchFileWriter(file, table.schema) as writer:
         writer.write_table(table)


Listing files
~~~~~~~~~~~~~

Inspecting the directories and files on a filesystem can be done with the
:meth:`FileSystem.get_file_info` method. To list the contents of a directory,
use the :class:`FileSelector` object to specify the selection::

   >>> local.get_file_info(fs.FileSelector("dataset/", recursive=True))
   [<FileInfo for 'dataset/part=B': type=FileType.Directory>,
    <FileInfo for 'dataset/part=B/data0.parquet': type=FileType.File, size=1564>,
    <FileInfo for 'dataset/part=A': type=FileType.Directory>,
    <FileInfo for 'dataset/part=A/data0.parquet': type=FileType.File, size=1564>]

This returns a list of :class:`FileInfo` objects, containing information about
the type (file or directory), the size, the date last modified, etc.

You can also get this information for a single explicit path (or list of
paths)::

   >>> local.get_file_info('test.arrow')
   <FileInfo for 'test.arrow': type=FileType.File, size=3250>

   >>> local.get_file_info('non_existent')
   <FileInfo for 'non_existent': type=FileType.NotFound>


.. _filesystem-localfs:

Local FS
--------

The :class:`LocalFileSystem` allows you to access files on the local machine.

Example how to write to disk and read it back::

   >>> from pyarrow import fs
   >>> local = fs.LocalFileSystem()
   >>> with local.open_output_stream('/tmp/pyarrowtest.dat') as stream:
           stream.write(b'data')
   4
   >>> with local.open_input_stream('/tmp/pyarrowtest.dat') as stream:
           print(stream.readall())
   b'data'


.. _filesystem-s3:

S3
--

PyArrow implements natively a S3 filesystem for S3 compatible storage.

The :class:`S3FileSystem` constructor has several options to configure the S3
connection (e.g. credentials, the region, an endpoint override, etc). In
addition, the constructor will also inspect configured S3 credentials as
supported by AWS (for example the ``AWS_ACCESS_KEY_ID`` and
``AWS_SECRET_ACCESS_KEY`` environment variables).


Example how you can read contents from a S3 bucket::

   >>> from pyarrow import fs
   >>> s3 = fs.S3FileSystem(region='eu-west-3')

   # List all contents in a bucket, recursively
   >>> s3.get_file_info(fs.FileSelector('my-test-bucket', recursive=True))
   [<FileInfo for 'my-test-bucket/File1': type=FileType.File, size=10>,
    <FileInfo for 'my-test-bucket/File5': type=FileType.File, size=10>,
    <FileInfo for 'my-test-bucket/Dir1': type=FileType.Directory>,
    <FileInfo for 'my-test-bucket/Dir2': type=FileType.Directory>,
    <FileInfo for 'my-test-bucket/EmptyDir': type=FileType.Directory>,
    <FileInfo for 'my-test-bucket/Dir1/File2': type=FileType.File, size=11>,
    <FileInfo for 'my-test-bucket/Dir1/Subdir': type=FileType.Directory>,
    <FileInfo for 'my-test-bucket/Dir2/Subdir': type=FileType.Directory>,
    <FileInfo for 'my-test-bucket/Dir2/Subdir/File3': type=FileType.File, size=10>]

   # Open a file for reading and download its contents
   >>> f = s3.open_input_stream('my-test-bucket/Dir1/File2')
   >>> f.readall()
   b'some data'


Note that it is important to configure :class:`S3FileSystem` with the correct
region for the bucket being used. If `region` is not set, the AWS SDK will
choose a value, defaulting to 'us-east-1' if the SDK version is <1.8.
Otherwise it will try to use a variety of heuristics (environment variables,
configuration profile, EC2 metadata server) to resolve the region.

It is also possible to resolve the region from the bucket name for
:class:`S3FileSystem` by using :func:`pyarrow.fs.resolve_s3_region` or
:func:`pyarrow.fs.S3FileSystem.from_uri`.

Here are a couple examples in code::

   >>> from pyarrow import fs
   >>> s3 = fs.S3FileSystem(region=fs.resolve_s3_region('my-test-bucket'))

   # Or via URI:
   >>> s3, path = fs.S3FileSystem.from_uri('s3://[access_key:secret_key@]bucket/path]')


.. seealso::

   See the `AWS docs <https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/credentials.html>`__
   for the different ways to configure the AWS credentials.

   :func:`pyarrow.fs.resolve_s3_region` for resolving region from a bucket name.


.. _filesystem-gcs:

Google Cloud Storage File System
--------------------------------

PyArrow implements natively a Google Cloud Storage (GCS) backed file system
for GCS storage.

If not running on Google Cloud Platform (GCP), this generally requires the
environment variable ``GOOGLE_APPLICATION_CREDENTIALS`` to point to a
JSON file containing credentials. Alternatively, use the ``gcloud`` CLI to
generate a credentials file in the default location::

   gcloud auth application-default login

To connect to a public bucket without using any credentials, you must pass
``anonymous=True`` to :class:`GcsFileSystem`. Otherwise, the filesystem
will report ``Couldn't resolve host name`` since there are different host 
names for authenticated and public access.

Example showing how you can read contents from a GCS bucket::

   >>> from datetime import timedelta
   >>> from pyarrow import fs
   >>> gcs = fs.GcsFileSystem(anonymous=True, retry_time_limit=timedelta(seconds=15))

   # List all contents in a bucket, recursively
   >>> uri = "gcp-public-data-landsat/LC08/01/001/003/"
   >>> file_list = gcs.get_file_info(fs.FileSelector(uri, recursive=True))

   # Open a file for reading and download its contents
   >>> f = gcs.open_input_stream(file_list[0].path)
   >>> f.read(64)
   b'GROUP = FILE_HEADER\n  LANDSAT_SCENE_ID = "LC80010032013082LGN03"\n  S'

.. seealso::

   The :class:`GcsFileSystem` constructor by default uses the
   process described in `GCS docs <https://google.aip.dev/auth/4110>`__
   to resolve credentials.


.. _filesystem-hdfs:

Hadoop Distributed File System (HDFS)
-------------------------------------

PyArrow comes with bindings to the Hadoop File System (based on C++ bindings
using ``libhdfs``, a JNI-based interface to the Java Hadoop client). You connect
using the :class:`HadoopFileSystem` constructor:

.. code-block:: python

   from pyarrow import fs
   hdfs = fs.HadoopFileSystem(host, port, user=user, kerb_ticket=ticket_cache_path)

The ``libhdfs`` library is loaded **at runtime** (rather than at link / library
load time, since the library may not be in your LD_LIBRARY_PATH), and relies on
some environment variables.

* ``HADOOP_HOME``: the root of your installed Hadoop distribution. Often has
  `lib/native/libhdfs.so`.

* ``JAVA_HOME``: the location of your Java SDK installation.

* ``ARROW_LIBHDFS_DIR`` (optional): explicit location of ``libhdfs.so`` if it is
  installed somewhere other than ``$HADOOP_HOME/lib/native``.

* ``CLASSPATH``: must contain the Hadoop jars. You can set these using:

  .. code-block:: shell

      export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
      # or on Windows
      %HADOOP_HOME%/bin/hadoop classpath --glob > %CLASSPATH%

  In contrast to the legacy HDFS filesystem with ``pa.hdfs.connect``, setting
  ``CLASSPATH`` is not optional (pyarrow will not attempt to infer it).

.. _filesystem-fsspec:

Using fsspec-compatible filesystems with Arrow
----------------------------------------------

The filesystems mentioned above are natively supported by Arrow C++ / PyArrow.
The Python ecosystem, however, also has several filesystem packages. Those
packages following the `fsspec`_ interface can be used in PyArrow as well.

Functions accepting a filesystem object will also accept an fsspec subclass.
For example::

   # creating an fsspec-based filesystem object for Google Cloud Storage
   import gcsfs
   fs = gcsfs.GCSFileSystem(project='my-google-project')

   # using this to read a partitioned dataset
   import pyarrow.dataset as ds
   ds.dataset("data/", filesystem=fs)
   
Similarly for Azure Blob Storage::

   import adlfs
   # ... load your credentials and configure the filesystem
   fs = adlfs.AzureBlobFileSystem(account_name=account_name, account_key=account_key)

   import pyarrow.dataset as ds
   ds.dataset("mycontainer/data/", filesystem=fs)

Under the hood, the fsspec filesystem object is wrapped into a python-based
PyArrow filesystem (:class:`PyFileSystem`) using :class:`FSSpecHandler`.
You can also manually do this to get an object with the PyArrow FileSystem
interface::

   from pyarrow.fs import PyFileSystem, FSSpecHandler
   pa_fs = PyFileSystem(FSSpecHandler(fs))

Then all the functionalities of :class:`FileSystem` are accessible::

   # write data
   with pa_fs.open_output_stream('mycontainer/pyarrowtest.dat') as stream:
      stream.write(b'data')

   # read data
   with pa_fs.open_input_stream('mycontainer/pyarrowtest.dat') as stream:
      print(stream.readall())
   #b'data'

   # read a partitioned dataset
   ds.dataset("data/", filesystem=pa_fs)


Using Arrow filesystems with fsspec
-----------------------------------

The Arrow FileSystem interface has a limited, developer-oriented API surface.
This is sufficient for basic interactions and for using this with
Arrow's IO functionality. On the other hand, the `fsspec`_ interface provides
a very large API with many helper methods. If you want to use those, or if you
need to interact with a package that expects fsspec-compatible filesystem
objects, you can wrap an Arrow FileSystem object with fsspec.

Starting with ``fsspec`` version 2021.09, the ``ArrowFSWrapper`` can be used
for this::

   >>> from pyarrow import fs
   >>> local = fs.LocalFileSystem()
   >>> from fsspec.implementations.arrow import ArrowFSWrapper
   >>> local_fsspec = ArrowFSWrapper(local)

The resulting object now has an fsspec-compatible interface, while being backed
by the Arrow FileSystem under the hood.
Example usage to create a directory and file, and list the content::

   >>> local_fsspec.mkdir("./test")
   >>> local_fsspec.touch("./test/file.txt")
   >>> local_fsspec.ls("./test/")
   ['./test/file.txt']

For more information, see the `fsspec`_ documentation.


.. _fsspec: https://filesystem-spec.readthedocs.io/en/latest/

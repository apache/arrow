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

File System Interfaces
======================

In this section, we discuss filesystem-like interfaces in PyArrow.

.. _hdfs:

Hadoop File System (HDFS)
-------------------------

PyArrow comes with bindings to a C++-based interface to the Hadoop File
System. You connect like so:

.. code-block:: python

   import pyarrow as pa
   fs = pa.hdfs.connect(host, port, user=user, kerb_ticket=ticket_cache_path)
   with fs.open(path, 'rb') as f:
       # Do something with f

By default, ``pyarrow.hdfs.HadoopFileSystem`` uses libhdfs, a JNI-based
interface to the Java Hadoop client. This library is loaded **at runtime**
(rather than at link / library load time, since the library may not be in your
LD_LIBRARY_PATH), and relies on some environment variables.

* ``HADOOP_HOME``: the root of your installed Hadoop distribution. Often has
  `lib/native/libhdfs.so`.

* ``JAVA_HOME``: the location of your Java SDK installation.

* ``ARROW_LIBHDFS_DIR`` (optional): explicit location of ``libhdfs.so`` if it is
  installed somewhere other than ``$HADOOP_HOME/lib/native``.

* ``CLASSPATH``: must contain the Hadoop jars. You can set these using:

.. code-block:: shell

    export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob`

If ``CLASSPATH`` is not set, then it will be set automatically if the
``hadoop`` executable is in your system path, or if ``HADOOP_HOME`` is set.

You can also use libhdfs3, a thirdparty C++ library for HDFS from Pivotal Labs:

.. code-block:: python

   fs = pa.hdfs.connect(host, port, user=user, kerb_ticket=ticket_cache_path,
                       driver='libhdfs3')

HDFS API
~~~~~~~~

.. currentmodule:: pyarrow

.. autosummary::
   :toctree: generated/

   hdfs.connect
   HadoopFileSystem.cat
   HadoopFileSystem.chmod
   HadoopFileSystem.chown
   HadoopFileSystem.delete
   HadoopFileSystem.df
   HadoopFileSystem.disk_usage
   HadoopFileSystem.download
   HadoopFileSystem.exists
   HadoopFileSystem.get_capacity
   HadoopFileSystem.get_space_used
   HadoopFileSystem.info
   HadoopFileSystem.ls
   HadoopFileSystem.mkdir
   HadoopFileSystem.open
   HadoopFileSystem.rename
   HadoopFileSystem.rm
   HadoopFileSystem.upload
   HdfsFile

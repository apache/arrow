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

Filesystem Interfaces
=====================

In this section, we discuss filesystem-like interfaces in PyArrow.

.. _hdfs:

Hadoop File System (HDFS)
-------------------------

PyArrow comes with bindings to a C++-based interface to the Hadoop File
System. You connect like so:

.. code-block:: python

   import pyarrow as pa
   hdfs = pa.HdfsClient(host, port, user=user, kerb_ticket=ticket_cache_path)

By default, ``pyarrow.HdfsClient`` uses libhdfs, a JNI-based interface to the
Java Hadoop client. This library is loaded **at runtime** (rather than at link
/ library load time, since the library may not be in your LD_LIBRARY_PATH), and
relies on some environment variables.

* ``HADOOP_HOME``: the root of your installed Hadoop distribution. Often has
  `lib/native/libhdfs.so`.

* ``JAVA_HOME``: the location of your Java SDK installation.

* ``ARROW_LIBHDFS_DIR`` (optional): explicit location of ``libhdfs.so`` if it is
  installed somewhere other than ``$HADOOP_HOME/lib/native``.

* ``CLASSPATH``: must contain the Hadoop jars. You can set these using:

.. code-block:: shell

    export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob`

You can also use libhdfs3, a thirdparty C++ library for HDFS from Pivotal Labs:

.. code-block:: python

   hdfs3 = pa.HdfsClient(host, port, user=user, kerb_ticket=ticket_cache_path,
                         driver='libhdfs3')

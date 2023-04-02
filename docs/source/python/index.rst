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

PyArrow - Apache Arrow Python bindings
======================================

This is the documentation of the Python API of Apache Arrow.

Apache Arrow is a development platform for in-memory analytics.
It contains a set of technologies that enable big data systems to store, process and move data fast.

See the :doc:`parent documentation <../index>` for additional details on
the Arrow Project itself, on the Arrow format and the other language bindings.

The Arrow Python bindings (also named "PyArrow") have first-class integration
with NumPy, pandas, and built-in Python objects. They are based on the C++
implementation of Arrow.

Here we will detail the usage of the Python API for Arrow and the leaf
libraries that add additional functionality such as reading Apache Parquet
files into Arrow structures.

.. toctree::
   :maxdepth: 2

   install
   getstarted
   data
   compute
   memory
   ipc
   filesystems
   filesystems_deprecated
   numpy
   pandas
   timestamps
   orc
   csv
   feather
   json
   parquet
   dataset
   flight
   extending_types
   integration
   env_vars
   api
   getting_involved
   benchmarks

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

Python bindings
===============

The Arrow Python bindings have first-class integration with NumPy, pandas, and
built-in Python objects. They are based on the C++ implementation of Arrow.

This is the documentation of the Python API of Apache Arrow. For more details
on the format and other language bindings see the parent documentation.
Here will we only detail the usage of the Python API for Arrow and the leaf
libraries that add additional functionality such as reading Apache Parquet
files into Arrow structures.

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   install
   development
   memory
   data
   ipc
   filesystems
   plasma
   numpy
   pandas
   csv
   parquet
   extending
   api
   getting_involved


Apache Arrow C++ API documentation      {#index}
==================================

<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

Apache Arrow is a columnar in-memory analytics layer designed to accelerate
big data. It houses a set of canonical in-memory representations of flat and
hierarchical data along with multiple language-bindings for structure
manipulation. It also provides IPC and common algorithm implementations.

This is the documentation of the C++ API of Apache Arrow. For more details
on the format and other language bindings see
the [main page for Arrow](https://arrow.apache.org/). Here will we only detail
the usage of the C++ API for Arrow and the leaf libraries that add additional
functionality such as using [jemalloc](http://jemalloc.net/) as an allocator
for Arrow structures.

Table of Contents
-----------------

 * Instructions on how to build Arrow C++ on [Windows](Windows.md)
 * How to access [HDFS](HDFS.md)
 * Tutorials
   * [Using the Plasma In-Memory Object Store](tutorials/plasma.md)
   * [Use Plasma to Access Tensors from C++ in Python](tutorials/tensor_to_py.md)

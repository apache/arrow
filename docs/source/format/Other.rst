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

Other Data Structures
=====================

Our Flatbuffers protocol files have metadata for some other data
structures defined to allow other kinds of applications to take
advantage of common interprocess communication machinery. These data
structures are not considered to be part of the columnar format.

An Arrow columnar implementation is not required to implement these
types.

Tensor (Multi-dimensional Array)
--------------------------------

The ``Tensor`` message types provides a way to write a
multidimensional array of fixed-size values (such as a NumPy ndarray).

When writing a standalone encapsulated tensor message, we use the format as
indicated above, but additionally align the starting offset of the metadata as
well as the starting offset of the tensor body (if writing to a shared memory
region) to be multiples of 64 bytes: ::

    <PADDING>
    <metadata size: int32>
    <metadata>
    <tensor body>

Sparse Tensor
-------------

``SparseTensor`` represents a multidimensional array whose elements
are generally almost all zeros.

When writing a standalone encapsulated sparse tensor message, we use
the format as indicated above, but additionally align the starting
offset of the metadata as well as the starting offsets of the sparse
index and the sparse tensor body (if writing to a shared memory
region) to be multiples of 64 bytes: ::

    <PADDING>
    <metadata size: int32>
    <metadata>
    <sparse index>
    <PADDING>
    <sparse tensor body>

The contents of the sparse tensor index is depends on what kinds of sparse
format is used.

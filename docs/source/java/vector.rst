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

===========
ValueVector
===========

:class:`ValueVector` interface (which called Array in C++ implementation and
the :doc:`the specification <../format/Columnar>`) is an abstraction that is used to store a
sequence of values having the same type in an individual column. Internally, those values are
represented by one or several buffers, the number and meaning of which depend on the vector’s data type.

There are concrete subclasses of :class:`ValueVector` for each primitive data type
and nested type described in the specification. There are a few differences in naming
with the type names described in the specification:
Table with non-intuitive names (BigInt = 64 bit integer, etc).

It is important that vector is allocated before attempting to read or write,
:class:`ValueVector` "should" strive to guarantee this order of operation:
allocate > mutate > set valuecount > access > clear (or allocate to start the process over)

Building ValueVector
====================

Note that the current implementation doesn't enforce the rule that Arrow objects are immutable.
:class:`ValueVector` instances could be created directly by using new keyword, there are
set/setSafe APIs and concrete subclasses of FieldWriter for populating values.

For example, the code below shows how to build a :class:`BigIntVector`, in this case, we build a
vector of the range 0 to 7 where the element that should hold the fourth value is nulled::

   BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

   BigIntVector vector = new BigIntVector("vector", allocator);
   vector.allocateNew(8);
   vector.set(0, 1);
   vector.set(1, 2);
   vector.set(2, 3);
   vector.setNull(3);
   vector.set(4, 5);
   vector.set(5, 6);
   vector.set(6, 7);
   vector.set(7, 8);
   vector.setValueCount(8); // this will finalizes the vector by convention.

The :class:`BigIntVector` holds two ArrowBufs. The first buffer holds the null bitmap, which consists
here of a single byte with the bits 1|1|1|1|0|1|1|1 (the bit is 1 if the value is non-null).
The second buffer contains all the above values. As the fourth entry is null, the value at that position
in the buffer is undefined. Note compared with set API, setSafe API would check value capacity before setting
values and reallocate buffers if necessary.

Here is how to build a vector using writer::

   BigIntVector vector = new BigIntVector("vector", allocator);
   BigIntWriter writer = new BigIntWriterImpl(vector);
   writer.setPosition(0);
   writer.writeBigInt(1);
   writer.setPosition(1);
   writer.writeBigInt(2);
   writer.setPosition(2);
   writer.writeBigInt(3);
   // writer.setPosition(3) is not called which means the forth value is null.
   writer.setPosition(4);
   writer.writeBigInt(5);
   writer.setPosition(5);
   writer.writeBigInt(6);
   writer.setPosition(6);
   writer.writeBigInt(7);
   writer.setPosition(7);
   writer.writeBigInt(8);

There are get API and concrete subclasses of :class:`FieldReader` for accessing vector values, what needs
to be declared is that writer/reader is not as efficient as direct access::

    // access via get API
    for (int i = 0; i < vector.getValueCount(); i++) {
      if (!vector.isNull(i)) {
        System.out.println(vector.get(i));
      }
    }

    // access via reader
    BigIntReader reader = vector.getReader();
    for (int i = 0; i < vector.getValueCount(); i++) {
      reader.setPosition(i);
      if (reader.isSet()) {
        System.out.println(reader.readLong());
      }
    }


Slicing
=======
Similar with C++ implementation, it is possible to make zero-copy slices of vectors to obtain a vector
referring to some logical sub-sequence of the data through :class:`TransferPair`::

    IntVector vector = new IntVector("intVector", allocator);
    for (int i = 0; i < 10; i++) {
      vector.setSafe(i, i);
    }
    vector.setValueCount(10);

    TransferPair tp = vector.getTransferPair(allocator);
    tp.splitAndTransfer(0, 5);
    IntVector sliced = (IntVector) tp.getTo();
    // In this case, the vector values are [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] and the sliceVector values are [0, 1, 2, 3, 4].


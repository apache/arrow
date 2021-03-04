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
create > allocate > mutate > set value count > access > clear (or allocate to start the process over).
We will go through a concrete example to demonstrate each operation in the next section.

Vector Life Cycle
=================

As discussed above, each vector goes through several steps in its life cycle,
and each step is triggered by a vector operation. In particular, we have the following vector operations:

1. **Vector creation**: we create a new vector object by, for example, the vector constructor.
The following code creates a new ``IntVector`` by the constructor:

.. code-block:: Java

    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ...
    IntVector vector = new IntVector("int vector", allocator);

By now, a vector object is created. However, no underlying memory has been allocated, so we need the
following step.

2. **Vector allocation**: in this step, we allocate memory for the vector. For most vectors, we
have two options: 1) if we know the maximum vector capacity, we can specify it by calling the
``allocateNew(int)`` method; 2) otherwise, we should call the ``allocateNew()`` method, and  a default
capacity will be allocated for it. For our running example, we assume that the vector capacity never
exceeds 10:

.. code-block:: Java

    vector.allocateNew(10);

3. **Vector mutation**: now we can populate the vector with values we desire. For all vectors, we can populate
vector values through vector writers (An example will be given in the next section). For primitive types,
we can also mutate the vector by the set methods. There are two classes of set methods: 1) if we can
be sure the vector has enough capacity, we can call the ``set(index, value)`` method. 2) if we are not sure
about the vector capacity, we should call the ``setSafe(index, value)`` method, which will automatically
take care of vector reallocation, if the capacity is not sufficient. For our running example, we know the
vector has enough capacity, so we can call

.. code-block:: Java

    vector.set(/*index*/5, /*value*/25);

4. **Set value count**: for this step, we set the value count of the vector by calling the
``setValueCount(int)`` method:

.. code-block:: Java

    vector.setValueCount(10);

After this step, the vector enters an immutable state. In other words, we should no longer mutate it.
(Unless we reuse the vector by allocating it again. This will be discussed shortly.)

5. **Vector access**: it is time to access vector values. Similarly, we have two options to access values:
1) get methods and 2) vector reader. Vector reader works for all types of vectors, while get methods are
only available for primitive vectors. A concrete example for vector reader will be given in the next section.
Below is an example of vector access by get method:

.. code-block:: Java

    int value = vector.get(5);  // value == 25

6. **Vector clear**: when we are done with the vector, we should clear it to release its memory. This is done by
calling the ``close()`` method:

.. code-block:: Java

    vector.close();

Some points to note about the steps above:

* The steps are not necessarily performed in a linear sequence. Instead, they can be in a loop. For example,
  when a vector enters the access step, we can also go back to the vector mutation step, and then set value
  count, access vector, and so on.

* We should try to make sure the above steps are carried out in order. Otherwise, the vector
  may be in an undefined state, and some unexpected behavior may occur. However, this restriction
  is not strict. That means it is possible that we violates the order above, but still get
  correct results.

* When mutating vector values through set methods, we should prefer ``set(index, value)`` methods to
  ``setSafe(index, value)`` methods whenever possible, to avoid unnecessary performance overhead of handling
  vector capacity.

* All vectors implement the ``AutoCloseable`` interface. So they must be closed explicitly when they are
  no longer used, to avoid resource leak. To make sure of this, it is recommended to place vector related operations
  into a try-with-resources block.

* For fixed width vectors (e.g. IntVector), we can set values at different indices in arbitrary orders.
  For variable width vectors (e.g. VarCharVector), however, we must set values in non-decreasing order of the
  indices. Otherwise, the values after the set position will become invalid. For example, suppose we use the
  following statements to populate a variable width vector:

.. code-block:: Java

    VarCharVector vector = new VarCharVector("vector", allocator);
    vector.allocateNew();
    vector.setSafe(0, "zero");
    vector.setSafe(1, "one");
    ...
    vector.setSafe(9, "nine");

Then we set the value at position 5 again:

.. code-block:: Java

    vector.setSafe(5, "5");

After that, the values at positions 6, 7, 8, and 9 of the vector will become invalid.

Building ValueVector
====================

Note that the current implementation doesn't enforce the rule that Arrow objects are immutable.
:class:`ValueVector` instances could be created directly by using new keyword, there are
set/setSafe APIs and concrete subclasses of FieldWriter for populating values.

For example, the code below shows how to build a :class:`BigIntVector`, in this case, we build a
vector of the range 0 to 7 where the element that should hold the fourth value is nulled

.. code-block:: Java

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
      BigIntVector vector = new BigIntVector("vector", allocator)) {
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
      ...
    }

The :class:`BigIntVector` holds two ArrowBufs. The first buffer holds the null bitmap, which consists
here of a single byte with the bits 1|1|1|1|0|1|1|1 (the bit is 1 if the value is non-null).
The second buffer contains all the above values. As the fourth entry is null, the value at that position
in the buffer is undefined. Note compared with set API, setSafe API would check value capacity before setting
values and reallocate buffers if necessary.

Here is how to build a vector using writer

.. code-block:: Java

    try (BigIntVector vector = new BigIntVector("vector", allocator);
      BigIntWriter writer = new BigIntWriterImpl(vector)) {
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
    }

There are get API and concrete subclasses of :class:`FieldReader` for accessing vector values, what needs
to be declared is that writer/reader is not as efficient as direct access

.. code-block:: Java

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

Building ListVector
===================

A :class:`ListVector` is a vector that holds a list of values for each index. Working with one you need to handle the same steps as mentioned above (create > allocate > mutate > set value count > access > clear), but the details of how you accomplish this are slightly different since you need to both create the vector and set the list of values for each index.

For example, the code below shows how to build a :class:`ListVector` of int's using the writer :class:`UnionListWriter`. We build a vector from 0 to 9 and each index contains a list with values [[0, 0, 0, 0, 0], [0, 1, 2, 3, 4], [0, 2, 4, 6, 8], …, [0, 9, 18, 27, 36]]. List values can be added in any order so writing a list such as [3, 1, 2] would be just as valid.

.. code-block:: Java
  
  try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ListVector listVector = ListVector.empty("vector", allocator)) {
    UnionListWriter writer = listVector.getWriter();
    for (int i = 0; i < 10; i++) {
       writer.startList();
       writer.setPosition(i);
       for (int j = 0; j < 5; j++) {
           writer.writeInt(j * i);
       }
       writer.setValueCount(5);
       writer.endList();
    }
    listVector.setValueCount(10);
  }    

:class:`ListVector` values can be accessed either through the get API or through the reader class :class:`UnionListReader`. To read all the values, first enumerate through the indexes, and then enumerate through the inner list values.

.. code-block:: Java

  // access via get API
  for (int i = 0; i < listVector.getValueCount(); i++) {
     if (!listVector.isNull(i)) {
         ArrayList<Integer> elements = (ArrayList<Integer>) listVector.getObject(i);
         for (Integer element : elements) {
             System.out.println(element);
         }
     }
  }

  // access via reader
  UnionListReader reader = listVector.getReader();
  for (int i = 0; i < listVector.getValueCount(); i++) {
     reader.setPosition(i);
     while (reader.next()) {
         IntReader intReader = reader.reader();
         if (intReader.isSet()) {
             System.out.println(intReader.readInteger());
         }
     }
  }

Slicing
=======

Similar with C++ implementation, it is possible to make zero-copy slices of vectors to obtain a vector
referring to some logical sub-sequence of the data through :class:`TransferPair`

.. code-block:: Java

    IntVector vector = new IntVector("intVector", allocator);
    for (int i = 0; i < 10; i++) {
      vector.setSafe(i, i);
    }
    vector.setValueCount(10);

    TransferPair tp = vector.getTransferPair(allocator);
    tp.splitAndTransfer(0, 5);
    IntVector sliced = (IntVector) tp.getTo();
    // In this case, the vector values are [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] and the sliceVector values are [0, 1, 2, 3, 4].

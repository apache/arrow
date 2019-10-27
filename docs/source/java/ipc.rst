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

===========================
Reading/Writing IPC formats
===========================
Arrow defines two types of binary formats for serializing record batches:

* **Streaming format**: for sending an arbitrary length sequence of record
  batches. The format must be processed from start to end, and does not support
  random access

* **File or Random Access format**: for serializing a fixed number of record
  batches. Supports random access, and thus is very useful when used with
  memory maps

Writing and Reading Streaming Format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
First, let's populate a :class:`VectorSchemaRoot` with a small batch of records::

    BitVector bitVector = new BitVector("boolean", allocator);
    VarCharVector varCharVector = new VarCharVector("varchar", allocator);
    for (int i = 0; i < 10; i++) {
      bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
      varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
    }
    bitVector.setValueCount(10);
    varCharVector.setValueCount(10);

    List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
    List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);
    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

Now, we can begin writing a stream containing some number of these batches. For this we use :class:`ArrowStreamWriter`
(DictionaryProvider used for any vectors that are dictionary encoded is optional and can be null))::

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ArrowStreamWriter writer = new ArrowStreamWriter(root, /*DictionaryProvider=*/null, Channels.newChannel(out));

Here we used an in-memory stream, but this could have been a socket or some other IO stream, then we can do::

    writer.start();
    writer.writeBatch();
    for (int i = 0; i < 4; i++) {
      // populate VectorSchemaRoot data
      BitVector childVector1 = (BitVector)root.getVector(0);
      VarCharVector childVector2 = (VarCharVector)root.getVector(1);
      childVector1.reset();
      childVector2.reset();
      ... do some populate work here, could be different for each batch

      writer.writeBatch();
    }
    writer.end();

Note since the :class:`VectorSchemaRoot` in writer is a container that can hold batches, batches flow through
:class:`VectorSchemaRoot` as part of a pipeline, so we need to populated data if necessary before non-first call,
otherwise several same batches will be written.

Now the :class:`ByteArrayOutputStream` contains the complete stream which contains 5 record batches.
We can read such a stream with :class:`ArrowStreamReader`, note that :class:`VectorSchemaRoot` within
reader will be loaded with new values on every call to :class:`loadNextBatch()`::

    try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
      Schema schema = reader.getVectorSchemaRoot().getSchema();
      for (int i = 0; i < 5; i++) {
        // This will be loaded with new values on every call to loadNextBatch
        VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
        assertTrue(reader.loadNextBatch());
        // do something with readBatch
      }

    }

Writing and Reading Random Access Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The :class:`ArrowFileWriter` has the same API as :class:`ArrowStreamWriter`::

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out));
    writer.start();
    writer.writeBatch();
    for (int i = 0; i < 4; i++) {
      ... do populate work
      writer.writeBatch();
    }
    writer.end();

The difference between :class:`ArrowFileReader` and :class:`ArrowStreamReader` is that the input source
must have a ``seek`` method for random access. Because we have access to the entire payload, we know the
number of record batches in the file, and can read any at random::

    try (ArrowFileReader reader = new ArrowFileReader(
        new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator)) {

      // read the 4-th batch
      ArrowBlock block = reader.getRecordBlocks().get(3);
      reader.loadRecordBatch(block);
      VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
    }

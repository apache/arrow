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

================
VectorSchemaRoot
================
A :class:`VectorSchemaRoot` is a collection of equal-length vector instances(which called RecordBatch
in other implementations), it also support zero-copy slices. Here is the example of building a
:class:`VectorSchemaRoot` and slicing operation::

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
    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(fields, vectors);

    // slice
    VectorSchemaRoot slicedRoot = vectorSchemaRoot.slice(0, 5);


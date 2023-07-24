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

=================
Quick Start Guide
=================

.. contents::

Arrow Java provides several building blocks. Data types describe the types of values;
ValueVectors are sequences of typed values; fields describe the types of columns in
tabular data; schemas describe a sequence of columns in tabular data, and
VectorSchemaRoot represents tabular data. Arrow also provides readers and
writers for loading data from and persisting data to storage.

Create a ValueVector
********************

**ValueVectors** represent a sequence of values of the same type.
They are also known as "arrays" in the columnar format.

Example: create a vector of 32-bit integers representing ``[1, null, 2]``:

.. code-block:: Java

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.IntVector;

    try(
        BufferAllocator allocator = new RootAllocator();
        IntVector intVector = new IntVector("fixed-size-primitive-layout", allocator);
    ){
        intVector.allocateNew(3);
        intVector.set(0,1);
        intVector.setNull(1);
        intVector.set(2,2);
        intVector.setValueCount(3);
        System.out.println("Vector created in memory: " + intVector);
    }

.. code-block:: shell

    Vector created in memory: [1, null, 2]


Example: create a vector of UTF-8 encoded strings representing ``["one", "two", "three"]``:

.. code-block:: Java

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;

    try(
        BufferAllocator allocator = new RootAllocator();
        VarCharVector varCharVector = new VarCharVector("variable-size-primitive-layout", allocator);
    ){
        varCharVector.allocateNew(3);
        varCharVector.set(0, "one".getBytes());
        varCharVector.set(1, "two".getBytes());
        varCharVector.set(2, "three".getBytes());
        varCharVector.setValueCount(3);
        System.out.println("Vector created in memory: " + varCharVector);
    }

.. code-block:: shell

    Vector created in memory: [one, two, three]

Create a Field
**************

**Fields** are used to denote the particular columns of tabular data.
They consist of a name, a data type, a flag indicating whether the column can have null values,
and optional key-value metadata.

Example: create a field named "document" of string type:

.. code-block:: Java

    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import java.util.HashMap;
    import java.util.Map;

    Map<String, String> metadata = new HashMap<>();
    metadata.put("A", "Id card");
    metadata.put("B", "Passport");
    metadata.put("C", "Visa");
    Field document = new Field("document",
            new FieldType(true, new ArrowType.Utf8(), /*dictionary*/ null, metadata),
            /*children*/ null);
    System.out.println("Field created: " + document + ", Metadata: " + document.getMetadata());

.. code-block:: shell

    Field created: document: Utf8, Metadata: {A=Id card, B=Passport, C=Visa}

Create a Schema
***************

**Schemas** hold a sequence of fields together with some optional metadata.

Example: Create a schema describing datasets with two columns:
an int32 column "A" and a UTF8-encoded string column "B"

.. code-block:: Java

    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import java.util.HashMap;
    import java.util.Map;
    import static java.util.Arrays.asList;

    Map<String, String> metadata = new HashMap<>();
    metadata.put("K1", "V1");
    metadata.put("K2", "V2");
    Field a = new Field("A", FieldType.nullable(new ArrowType.Int(32, true)), /*children*/ null);
    Field b = new Field("B", FieldType.nullable(new ArrowType.Utf8()), /*children*/ null);
    Schema schema = new Schema(asList(a, b), metadata);
    System.out.println("Schema created: " + schema);

.. code-block:: shell

    Schema created: Schema<A: Int(32, true), B: Utf8>(metadata: {K1=V1, K2=V2})

Create a VectorSchemaRoot
*************************

A **VectorSchemaRoot** combines ValueVectors with a Schema to represent tabular data.

Example: Create a dataset of names (strings) and ages (32-bit signed integers).

.. code-block:: Java

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import java.nio.charset.StandardCharsets;
    import java.util.HashMap;
    import java.util.Map;
    import static java.util.Arrays.asList;

    Field age = new Field("age",
            FieldType.nullable(new ArrowType.Int(32, true)),
            /*children*/null
    );
    Field name = new Field("name",
            FieldType.nullable(new ArrowType.Utf8()),
            /*children*/null
    );
    Schema schema = new Schema(asList(age, name), /*metadata*/ null);
    try(
        BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        IntVector ageVector = (IntVector) root.getVector("age");
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
    ){
        root.setRowCount(3);
        ageVector.allocateNew(3);
        ageVector.set(0, 10);
        ageVector.set(1, 20);
        ageVector.set(2, 30);
        nameVector.allocateNew(3);
        nameVector.set(0, "Dave".getBytes(StandardCharsets.UTF_8));
        nameVector.set(1, "Peter".getBytes(StandardCharsets.UTF_8));
        nameVector.set(2, "Mary".getBytes(StandardCharsets.UTF_8));
        System.out.println("VectorSchemaRoot created: \n" + root.contentToTSVString());
    }

.. code-block:: shell

    VectorSchemaRoot created:
    age	    name
    10	    Dave
    20	    Peter
    30	    Mary


Interprocess Communication (IPC)
********************************

Arrow data can be written to and read from disk, and both of these can be done in
a streaming and/or random-access fashion depending on application requirements.

**Write data to an arrow file**

Example: Write the dataset from the previous example to an Arrow IPC file (random-access).

.. code-block:: Java

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.ipc.ArrowFileWriter;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import java.io.File;
    import java.io.FileOutputStream;
    import java.io.IOException;
    import java.nio.charset.StandardCharsets;
    import java.util.HashMap;
    import java.util.Map;
    import static java.util.Arrays.asList;

    Field age = new Field("age",
            FieldType.nullable(new ArrowType.Int(32, true)),
            /*children*/ null);
    Field name = new Field("name",
            FieldType.nullable(new ArrowType.Utf8()),
            /*children*/ null);
    Schema schema = new Schema(asList(age, name));
    try(
        BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        IntVector ageVector = (IntVector) root.getVector("age");
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
    ){
        ageVector.allocateNew(3);
        ageVector.set(0, 10);
        ageVector.set(1, 20);
        ageVector.set(2, 30);
        nameVector.allocateNew(3);
        nameVector.set(0, "Dave".getBytes(StandardCharsets.UTF_8));
        nameVector.set(1, "Peter".getBytes(StandardCharsets.UTF_8));
        nameVector.set(2, "Mary".getBytes(StandardCharsets.UTF_8));
        root.setRowCount(3);
        File file = new File("random_access_file.arrow");
        try (
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            ArrowFileWriter writer = new ArrowFileWriter(root, /*provider*/ null, fileOutputStream.getChannel());
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
            System.out.println("Record batches written: " + writer.getRecordBlocks().size()
                    + ". Number of rows written: " + root.getRowCount());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

.. code-block:: shell

    Record batches written: 1. Number of rows written: 3

**Read data from an arrow file**

Example: Read the dataset from the previous example from an Arrow IPC file (random-access).

.. code-block:: Java

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowFileReader;
    import org.apache.arrow.vector.ipc.message.ArrowBlock;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import java.io.File;
    import java.io.FileInputStream;
    import java.io.FileOutputStream;
    import java.io.IOException;

    try(
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FileInputStream fileInputStream = new FileInputStream(new File("random_access_file.arrow"));
        ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator);
    ){
        System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
        for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
            reader.loadRecordBatch(arrowBlock);
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            System.out.println("VectorSchemaRoot read: \n" + root.contentToTSVString());
        }
    } catch (IOException e) {
        e.printStackTrace();
    }

.. code-block:: shell

    Record batches in file: 1
    VectorSchemaRoot read:
    age	    name
    10	    Dave
    20	    Peter
    30	    Mary

More examples available at `Arrow Java Cookbook`_.

.. _`Arrow Java Cookbook`: https://arrow.apache.org/cookbook/java
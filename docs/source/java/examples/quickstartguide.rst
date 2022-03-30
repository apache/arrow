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

.. default-domain:: java
.. highlight:: java

=================
Quick Start Guide
=================

.. contents::

Arrow Java provides several building blocks. Data types describe the types of values;
``ValueVectors`` are sequences of typed values; ``fields`` describe the types of columns in
tabular data; ``schemas`` describe a sequence of columns in tabular data, and
``VectorSchemaRoot`` represents tabular data. Arrow also provides ``readers`` and
``writers`` for loading data from and persisting data to storage.

Create a ValueVector
*********************

ValueVectors represent a sequence of values of the same type.
They are also known as "arrays" in the columnar format.

Example: create a vector of 32-bit integers representing ``[1, null, 2]``:

.. code-block:: Java

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.IntVector;

    try(BufferAllocator allocator = new RootAllocator();
        IntVector intVector = new IntVector("fixed-size-primitive-layout", rootAllocator)){
        intVector.allocateNew(3);
        intVector.set(0,1);
        intVector.setNull(1);
        intVector.set(2,2);
        intVector.setValueCount(3);
        System.out.println("Vector created in memory: " + intVector);
    }

Example: create a vector of UTF-8 encoded strings representing ``["one", "two", "three"]``:

.. code-block:: Java

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;

    try(BufferAllocator rootAllocator = new RootAllocator();
        VarCharVector varCharVector = new VarCharVector("variable-size-primitive-layout", rootAllocator)){
        varCharVector.allocateNew(3);
        varCharVector.set(0, "one".getBytes());
        varCharVector.set(1, "two".getBytes());
        varCharVector.set(2, "three".getBytes());
        varCharVector.setValueCount(3);
        System.out.println("Vector created in memory: " + varCharVector);
    }

Create a Field
**************

Fields are used to denote the particular columns of tabular data.
They consist of a name, a data type, a flag indicating whether the column can have null values, and optional key-value metadata.

Example: create a field named "document" of string type:

.. code-block:: Java

    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;

    Map<String, String> metadata = new HashMap<>();
    metadata.put("A", "Id card");
    metadata.put("B", "Passport");
    metadata.put("C", "Visa");
    Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), /*dictionary*/ null, metadata), /*children*/ null);

Create a Schema
***************

Schema holds a sequence of fields together with some optional metadata.

**Schema**: Create a schema describing datasets with two columns:
a int32 column "A" and a utf8-encoded string column "B"

.. code-block:: Java

    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import static java.util.Arrays.asList;

    Map<String, String> metadata = new HashMap<>();
    metadata.put("K1", "V1");
    metadata.put("K2", "V2");
    Field a = new Field("A", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field b = new Field("B", FieldType.nullable(new ArrowType.Utf8()), null);
    Schema schema = new Schema(asList(a, b), metadata);

Create a VectorSchemaRoot
*************************

VectorSchemaRoot is somewhat analogous to tables and record batches in the other
Arrow implementations.

**VectorSchemaRoot**: Create a dataset with metadata that contains integer age and
string names of data.

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

    Map<String, String> metadataField = new HashMap<>();
    metadataField.put("K1-Field", "K1F1");
    metadataField.put("K2-Field", "K2F2");
    Field a = new Field("Column-A-Age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field b = new Field("Column-B-Name", new FieldType(true, new ArrowType.Utf8(), /*dictionary*/ null, metadataField), null);
    Map<String, String> metadataSchema = new HashMap<>();
    metadataSchema.put("K1-Schema", "K1S1");
    metadataSchema.put("K2-Schema", "K2S2");
    Schema schema = new Schema(asList(a, b), metadataSchema);
    System.out.println("Field A: " + a);
    System.out.println("Field B: " + b + ", Metadata: " + b.getMetadata());
    System.out.println("Schema: " + schema);
    try(BufferAllocator rootAllocator = new RootAllocator();
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)){
        vectorSchemaRoot.setRowCount(3);
        try(IntVector intVectorA = (IntVector) vectorSchemaRoot.getVector("Column-A-Age");
            VarCharVector varCharVectorB = (VarCharVector) vectorSchemaRoot.getVector("Column-B-Name")) {
            intVectorA.allocateNew(3);
            intVectorA.set(0, 10);
            intVectorA.set(1, 20);
            intVectorA.set(2, 30);

            varCharVectorB.allocateNew(3);
            varCharVectorB.set(0, "Dave".getBytes(StandardCharsets.UTF_8));
            varCharVectorB.set(1, "Peter".getBytes(StandardCharsets.UTF_8));
            varCharVectorB.set(2, "Mary".getBytes(StandardCharsets.UTF_8));

            System.out.println("VectorSchemaRoot: \n" + vectorSchemaRoot.contentToTSVString());
        }
    }

Interprocess Communication (IPC)
********************************

Arrow data can be written to and read from disk, and both of these can be done in
a streaming and/or random-access fashion depending on application requirements.

**Create a IPC File or Random Access Format**

Write File or Random Access Format: Write to a file a dataset with metadata
that contains integer age and string names of data.

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

    Map<String, String> metadataField = new HashMap<>();
    metadataField.put("K1-Field", "K1F1");
    metadataField.put("K2-Field", "K2F2");
    Field a = new Field("Column-A-Age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field b = new Field("Column-B-Name", new FieldType(true, new ArrowType.Utf8(), /*dictionary*/ null, metadataField), null);
    Map<String, String> metadataSchema = new HashMap<>();
    metadataSchema.put("K1-Schema", "K1S1");
    metadataSchema.put("K2-Schema", "K2S2");
    Schema schema = new Schema(asList(a, b), metadataSchema);
    System.out.println("Field A: " + a);
    System.out.println("Field B: " + b + ", Metadata: " + b.getMetadata());
    System.out.println("Schema: " + schema);
    try(BufferAllocator rootAllocator = new RootAllocator();
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)){
        vectorSchemaRoot.setRowCount(3);
        try(IntVector intVectorA = (IntVector) vectorSchemaRoot.getVector("Column-A-Age");
            VarCharVector varCharVectorB = (VarCharVector) vectorSchemaRoot.getVector("Column-B-Name")) {
            intVectorA.allocateNew(3);
            intVectorA.set(0, 10);
            intVectorA.set(1, 20);
            intVectorA.set(2, 30);
            varCharVectorB.allocateNew(3);
            varCharVectorB.set(0, "Dave".getBytes(StandardCharsets.UTF_8));
            varCharVectorB.set(1, "Peter".getBytes(StandardCharsets.UTF_8));
            varCharVectorB.set(2, "Mary".getBytes(StandardCharsets.UTF_8));
            // Arrow Java At Rest
            File file = new File("random_access_file.arrow");
            try (FileOutputStream fileOutputStream = new FileOutputStream(file);
                 ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel())
            ) {
                writer.start();
                writer.writeBatch();
                writer.end();
                System.out.println("Record batches written: " + writer.getRecordBlocks().size() + ". Number of rows written: " + vectorSchemaRoot.getRowCount());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

**Read a IPC File or Random Access Format**

Read File or Random Access Format: Mapping directly to memory a dataset file with metadata
that contains integer age and string names of data.

.. code-block:: Java

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowFileReader;
    import org.apache.arrow.vector.ipc.message.ArrowBlock;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import java.io.File;
    import java.io.FileInputStream;
    import java.io.FileOutputStream;
    import java.io.IOException;

    try(BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        File file = new File("random_access_file.arrow");
        try (FileInputStream fileInputStream = new FileInputStream(file);
             ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)
        ){
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
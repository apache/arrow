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

.. _java_modules:

=======
Modules
=======

.. contents::

This is the variety of arrow java documentation:

* `Specification and protocols`_: This contains agnostic specification that is implemented in this case by arrow java modules.
* `Supported environment`_ (like this): This contains answers for what-is-that arrow java module.
* `Cookbook`_: This contains answers about how-to-use arrow java modules with practices examples.
* `Development`_: This contains detailed information about what you need to consider to start with arrow java development.

Arrow java modules is created using specification such as columnar format, off-heap
memory, serialization and interprocess communication (IPC). Some of the java modules
was created with their own native implementations and others through bindings.

.. list-table:: Arrow Java Implementations and Bindings
   :widths: 25 75
   :header-rows: 1

   * - Module
     - Root Decision
   * - Arrow Memory
     - Native implementation.
   * - Arrow Vector
     - Native implementation.
   * - Arrow Flight Grpc
     - Native implementation.
   * - Arrow Flight Sql
     - Native implementation.
   * - Arrow Algorithm
     - Native implementation.
   * - Arrow Compression
     - Native implementation.
   * - Arrow C Data Interface
     - Native implementation.
   * - Arrow Dataset
     - Bindings
   * - Arrow ORC
     - Bindings
   * - Arrow Gandiva
     - Bindings

Arrow java is divided in these modules to offer in-memory columnar data structures:

.. list-table:: Arrow Java Modules
   :widths: 25 75
   :header-rows: 1

   * - Module
     - Description
   * - arrow-format
     - Generated Java files from the IPC Flatbuffer definitions.
   * - arrow-memory-core
     - Core off-heap memory management libraries for Arrow ValueVectors.
   * - arrow-memory-unsafe
     - Allocator and utils for allocating memory in Arrow based on sun.misc.Unsafe.
   * - arrow-memory-netty
     - Netty allocator and utils for allocating memory in Arrow.
   * - arrow-vector
     - An off-heap reference implementation for Arrow columnar data format.
   * - arrow-tools
     - Java applications for working with Arrow ValueVectors.
   * - arrow-jdbc
     - (`Experimental`) A library for converting JDBC data to Arrow data.
   * - arrow-plasma
     - (`Experimental`) Java client for the Plasma object store.
   * - flight-core
     - (`Experimental`) An RPC mechanism for transferring ValueVectors.
   * - flight-grpc
     - (`Experimental`) Contains utility class to expose Flight gRPC service and client
   * - flight-sql
     - (`Experimental`) Contains utility classes to expose Flight SQL semantics for clients and servers over Arrow Flight.
   * - flight-integration-tests
     - Integration tests for Flight RPC.
   * - arrow-performance
     - JMH Performance benchmarks for other Arrow libraries.
   * - arrow-algorithm
     - (`Experimental`) A collection of algorithms for working with ValueVectors.
   * - arrow-avro
     - (`Experimental`) A library for converting Avro data to Arrow data.
   * - arrow-compression
     - (`Experimental`) A library for working with the compression/decompression of Arrow data.
   * - arrow-c-data
     - Java implementation of C Data Interface
   * - arrow-orc
     - (`Experimental`) A JNI wrapper for the C++ ORC reader implementation.
   * - arrow-gandiva
     - Java wrappers around the native Gandiva SQL expression compiler.
   * - arrow-dataset
     - Java implementation of Arrow Dataset API/Framework

For more detail about how to install this modules please review
:doc:`Installing Java Modules <install>`.

.. note::

    Arrow java modules offer support to work data: (1) in-memory,
    (2) at rest and (3) on-the-wire.

Lets zoom-in arrow java modules to take advantage of this functionalities:

Arrow Java In-Memory (The Physical Layer)
-----------------------------------------

Off-Heap Memory
***************

Interact with the memory in java is a little bit complex, but interact
with direct-memory is a major challenge.

Java memory modules is based on `off-heap-memory` and offer helpers to
interact with direct memory in an easy way thru
their interfaces (i.e.: BufferAllocator).

Let's start with examples: Reserve 1MB of contiguous memory and assign
a value 112 at the index 5.

.. code-block:: Java

    import org.apache.arrow.memory.ArrowBuf;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;

    try(BufferAllocator bufferAllocator = new RootAllocator()){
        ArrowBuf arrowBuf = bufferAllocator.buffer(1024);
        arrowBuf.setInt(5, 112);
        System.out.println(arrowBuf);
        System.out.println(arrowBuf.getInt(5));
        arrowBuf.close();
    }

.. code-block:: shell

    ArrowBuf[2], address:140584424570880, length:1024
    112

For more detail on arrow java memory please review
:doc:`Memory Management <memory>`.

Columnar Format
***************

There is a detailed explanation about columnar format specification
at :doc:`Columnar Format <../format/Columnar>`.

This columnar data format specification is implemented by arrow java
vector module.

The One-Dimensional Layer
*************************

``Data Types``: Govern the `logical` interpretation of `physical` data.
Arrow java vector module use freemarker `template`_ to generate data types
defined on columnar format specification. On this resource you could see
implementation status of different data types `supported`_ by java vector module.

Each logical data type has a well-defined physical layout. Here are
the `different physical layouts defined by Arrow`_.

``Value Vector``: It's called `array` in the columnar format specification.
Value vector represent a one-dimensional sequence of homogeneous values.

Let's continue with examples of 1-D: How I could validate that arrow java vector
module implement arrow columnar format (For example a primitive array of int32s
[1, null, 2])?

.. code-block:: Java

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.IntVector;

    try(BufferAllocator rootAllocator = new RootAllocator();
        IntVector intVector = new IntVector("fixed-size-primiteve-layout", rootAllocator)){
        intVector.allocateNew(3);
        intVector.set(0,1);
        intVector.setNull(1);
        intVector.set(2,2);
        intVector.setValueCount(3);

        System.out.println("Vector created in memory: " + intVector);
        System.out.println("ArrowBuf validity: " + Integer.toBinaryString(intVector.getValidityBuffer().getInt(0)));
        System.out.println("ArrowBuf data[0]: " + intVector.get(0));
        System.out.println("ArrowBuf data[1]: " + intVector.isNull(1));
        System.out.println("ArrowBuf data[2]: " + intVector.get(2));
    }

.. code-block:: shell

    Vector created in memory: [1, null, 2]
    ArrowBuf validity: 101
    ArrowBuf data[0]: 1
    ArrowBuf data[1]: true
    ArrowBuf data[2]: 2

For more detail on arrow java vector please review :doc:`Value Vector <vector>`.

More examples available at `java cookbook create objects`_.

The Two-Dimensional Layer
*************************

Let's start talk about tabular data. Data often comes in the form of two-dimensional
sets of heterogeneous data (such as database tables, CSV files...). Arrow provides
several abstractions to handle such data conveniently and efficiently.

``Fields``: Fields are used to denote the particular columns of tabular data.

``Schema``: It holds a sequence of fields together with some optional metadata.

``VectorSchemaRoot``: It is somewhat analogous to tables and record batches in the
other Arrow implementations in that they all are 2D datasets, but the usage is different.

Let's continue with examples of 2-D: How could I create a dataset with metadata that
contains age and name data?

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

            System.out.println("Vector Schema Root: \n" + vectorSchemaRoot.contentToTSVString());
        }
    }

.. code-block:: shell

    Field A: Column-A-Age: Int(32, true)
    Field B: Column-B-Name: Utf8, Metadata: {K1-Field=K1F1, K2-Field=K2F2}
    Schema: Schema<Column-A-Age: Int(32, true), Column-B-Name: Utf8>(metadata: {K1-Schema=K1S1, K2-Schema=K2S2})
    Vector Schema Root:
    Column-A-Age	Column-B-Name
    10	                Dave
    20	                Peter
    30	                Mary

For more detail on arrow java vector please review :doc:`Vector Schema Root <vector_schema_root>`.

More examples available at `java cookbook working with schema`_.

Arrow Java At-Rest
------------------

The Arrow iInterprocess communication (IPC) format defines two types of binary formats
for serializing Arrow data:

``Streaming format``: For sending an arbitrary number of record batches. The format must
be processed from start to end, and does not support random access

``File or Random Access format``: For serializing a fixed number of record batches. It
supports random access.

This arrow java at rest specification is implemented by arrow java vector module.

Let's continue with examples of arrow java at rest: How do I could write the las dataset
created to a file?

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
            File file = new File("randon_access_to_file.arrow");
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

.. code-block:: shell

    Record batches written: 1. Number of rows written: 3

For more detail on arrow java io please review :doc:`Reading/Writing IPC formats <ipc>`.

More examples available at `java cookbook reading and writing data`_.

Arrow Java On-the-wire
----------------------

Arrow offer high performance data transport protocol through java ``flight`` module.
Arrow java flight is built using gRPC, protocol buffer and arrow columnar format,
it provides a framework for sending and receiving arrow data natively.

For more detail on arrow java on the wire please review :doc:`Arrow Flight RPC <../format/Flight>`
and :doc:`Arrow Flight SQL <../format/FlightSql>`.

More examples available at `java cookbook arrow flight`_.

.. _`Specification and protocols`: https://arrow.apache.org/docs/format/Versioning.html
.. _`Supported environment`: https://arrow.apache.org/docs/java/index.html
.. _`Cookbook`: https://arrow.apache.org/cookbook/java/index.html
.. _`Development`: https://arrow.apache.org/docs/developers/contributing.html
.. _`template`: https://github.com/apache/arrow/tree/master/java/vector/src/main/codegen/templates
.. _`supported`: https://arrow.apache.org/docs/status.html#data-types
.. _`different physical layouts defined by Arrow`: https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout
.. _`java cookbook create objects`: https://arrow.apache.org/cookbook/java/create.html
.. _`java cookbook working with schema`: https://arrow.apache.org/cookbook/java/schema.html
.. _`java cookbook reading and writing data`: https://arrow.apache.org/cookbook/java/io.html
.. _`java cookbook arrow flight`: https://arrow.apache.org/cookbook/java/flight.html
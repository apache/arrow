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

.. _java_quickstartguide:

=================
Quick Start Guide
=================

.. contents::

Arrow java manage data in Vector Schema Root (somewhat analogous to tables and record
batches in the other Arrow implementations). Before to create a Vector Schema Root let's
define another topics neededs for that purpose.

Create A Value Vector
*********************

It's called `array` in the columnar format specification. Represent a one-dimensional
sequence of homogeneous values.

**Int Vector**: Create an value vector of int32s like this [1, null, 2]

.. code-block:: Java

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.IntVector;

    try(BufferAllocator rootAllocator = new RootAllocator();
        IntVector intVector = new IntVector("fixed-size-primitive-layout", rootAllocator)){
        intVector.allocateNew(3);
        intVector.set(0,1);
        intVector.setNull(1);
        intVector.set(2,2);
        intVector.setValueCount(3);
        System.out.println("Vector created in memory: " + intVector);
    }

**Varchar Vector**: Create an value vector of string like this [one, two, three]

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

Create A Field
**************

Fields are used to denote the particular columns of tabular data.

**Field**: Create a column "document" of string type with metadata.

.. code-block:: Java

    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;

    Map<String, String> metadata = new HashMap<>();
    metadata.put("A", "Id card");
    metadata.put("B", "Passport");
    metadata.put("C", "Visa");
    Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), /*dictionary*/ null, metadata), /*children*/ null);

Create A Schema
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

Create A Vector Schema Root
***************************

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

            System.out.println("Vector Schema Root: \n" + vectorSchemaRoot.contentToTSVString());
        }
    }

Create a IPC File or Random Access Format
*****************************************

The Arrow Interprocess Communication (IPC) format defines two types of binary
formats for serializing Arrow data: the streaming format and the file format
(or random access format). Such files can be directly memory-mapped when read.

**Write File or Random Access Format**: Write to a file a dataset with metadata
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

Create A Flight Server And Flight Client
****************************************

Arrow java flight is built using gRPC, protocol buffer and arrow columnar format,
it provides a framework for sending and receiving arrow data natively.

**Flight**: Implement a service that provides a key-value store for data,
using Flight to handle uploads/requests and data in memory to store the actual data.

.. code-block:: Java

    import org.apache.arrow.flight.Action;
    import org.apache.arrow.flight.AsyncPutListener;
    import org.apache.arrow.flight.CallStatus;
    import org.apache.arrow.flight.Criteria;
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.FlightDescriptor;
    import org.apache.arrow.flight.FlightEndpoint;
    import org.apache.arrow.flight.FlightInfo;
    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.FlightStream;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.NoOpFlightProducer;
    import org.apache.arrow.flight.PutResult;
    import org.apache.arrow.flight.Result;
    import org.apache.arrow.flight.Ticket;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.VectorLoader;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.VectorUnloader;
    import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.Schema;

    import java.io.IOException;
    import java.nio.charset.StandardCharsets;
    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.Collections;
    import java.util.Iterator;
    import java.util.List;
    import java.util.concurrent.ConcurrentHashMap;

    class Dataset {
        private final List<ArrowRecordBatch> batches;
        private final Schema schema;
        private final long rows;
        public Dataset(List<ArrowRecordBatch> batches, Schema schema, long rows) {
            this.batches = batches;
            this.schema = schema;
            this.rows = rows;
        }
        public List<ArrowRecordBatch> getBatches() {
            return batches;
        }
        public Schema getSchema() {
            return schema;
        }
        public long getRows() {
            return rows;
        }
    }
    class CookbookProducer extends NoOpFlightProducer {
        private final RootAllocator allocator;
        private final Location location;
        private final ConcurrentHashMap<FlightDescriptor, Dataset> datasets;
        public CookbookProducer(RootAllocator allocator, Location location) {
            this.allocator = allocator;
            this.location = location;
            this.datasets = new ConcurrentHashMap<>();
        }
        @Override
        public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
            List<ArrowRecordBatch> batches = new ArrayList<>();
            return () -> {
                long rows = 0;
                VectorUnloader unloader;
                while (flightStream.next()) {
                    unloader = new VectorUnloader(flightStream.getRoot());
                    try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                        batches.add(arb);
                        rows += flightStream.getRoot().getRowCount();
                    }
                }
                Dataset dataset = new Dataset(batches, flightStream.getSchema(), rows);
                datasets.put(flightStream.getDescriptor(), dataset);
                ackStream.onCompleted();
            };
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            FlightDescriptor flightDescriptor = FlightDescriptor.path(
                    new String(ticket.getBytes(), StandardCharsets.UTF_8));
            Dataset dataset = this.datasets.get(flightDescriptor);
            if (dataset == null) {
                throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException();
            } else {
                VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(
                        this.datasets.get(flightDescriptor).getSchema(), allocator);
                listener.start(vectorSchemaRoot);
                for (ArrowRecordBatch arrowRecordBatch : this.datasets.get(flightDescriptor).getBatches()) {
                    VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                    loader.load(arrowRecordBatch.cloneWithTransfer(allocator));
                    listener.putNext();
                }
                listener.completed();
            }
        }

        @Override
        public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
            FlightDescriptor flightDescriptor = FlightDescriptor.path(
                    new String(action.getBody(), StandardCharsets.UTF_8));
            switch (action.getType()) {
                case "DELETE":
                    if (datasets.remove(flightDescriptor) != null) {
                        Result result = new Result("Delete completed".getBytes(StandardCharsets.UTF_8));
                        listener.onNext(result);
                    } else {
                        Result result = new Result("Delete not completed. Reason: Key did not exist."
                                .getBytes(StandardCharsets.UTF_8));
                        listener.onNext(result);
                    }
                    listener.onCompleted();
            }
        }

        @Override
        public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
            FlightEndpoint flightEndpoint = new FlightEndpoint(
                    new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)), location);
            return new FlightInfo(
                    datasets.get(descriptor).getSchema(),
                    descriptor,
                    Collections.singletonList(flightEndpoint),
                    /*bytes=*/-1,
                    datasets.get(descriptor).getRows()
            );
        }

        @Override
        public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
            datasets.forEach((k, v) -> { listener.onNext(getFlightInfo(null, k)); });
            listener.onCompleted();
        }
    }
    Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        // Server
        try(FlightServer flightServer = FlightServer.builder(allocator, location,
                new CookbookProducer(allocator, location)).build()) {
            try {
                flightServer.start();
                System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
            } catch (IOException e) {
                System.exit(1);
            }

            // Client
            try (FlightClient flightClient = FlightClient.builder(allocator, location).build()) {
                System.out.println("C1: Client (Location): Connected to " + location.getUri());

                // Populate data
                Schema schema = new Schema(Arrays.asList(
                        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
                try(VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
                    VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("name")) {
                    varCharVector.allocateNew(3);
                    varCharVector.set(0, "Ronald".getBytes());
                    varCharVector.set(1, "David".getBytes());
                    varCharVector.set(2, "Francisco".getBytes());
                    vectorSchemaRoot.setRowCount(3);
                    FlightClient.ClientStreamListener listener = flightClient.startPut(
                            FlightDescriptor.path("profiles"),
                            vectorSchemaRoot, new AsyncPutListener());
                    listener.putNext();
                    varCharVector.set(0, "Manuel".getBytes());
                    varCharVector.set(1, "Felipe".getBytes());
                    varCharVector.set(2, "JJ".getBytes());
                    vectorSchemaRoot.setRowCount(3);
                    listener.putNext();
                    listener.completed();
                    listener.getResult();
                    System.out.println("C2: Client (Populate Data): Wrote 2 batches with 3 rows each");
                }

                // Get metadata information
                FlightInfo flightInfo = flightClient.getInfo(FlightDescriptor.path("profiles"));
                System.out.println("C3: Client (Get Metadata): " + flightInfo);

                // Get data information
                try(FlightStream flightStream = flightClient.getStream(new Ticket(
                        FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)))) {
                    int batch = 0;
                    try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
                        System.out.println("C4: Client (Get Stream):");
                        while (flightStream.next()) {
                            batch++;
                            System.out.println("Client Received batch #" + batch + ", Data:");
                            System.out.print(vectorSchemaRootReceived.contentToTSVString());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Get all metadata information
                Iterable<FlightInfo> flightInfosBefore = flightClient.listFlights(Criteria.ALL);
                System.out.print("C5: Client (List Flights Info): ");
                flightInfosBefore.forEach(t -> System.out.println(t));

                // Do delete action
                Iterator<Result> deleteActionResult = flightClient.doAction(new Action("DELETE",
                        FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)));
                while (deleteActionResult.hasNext()) {
                    Result result = deleteActionResult.next();
                    System.out.println("C6: Client (Do Delete Action): " +
                            new String(result.getBody(), StandardCharsets.UTF_8));
                }

                // Get all metadata information (to validate detele action)
                Iterable<FlightInfo> flightInfos = flightClient.listFlights(Criteria.ALL);
                flightInfos.forEach(t -> System.out.println(t));
                System.out.println("C7: Client (List Flights Info): After delete - No records");

                // Server shut down
                flightServer.shutdown();
                System.out.println("C8: Server shut down successfully");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
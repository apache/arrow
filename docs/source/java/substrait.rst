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

=========
Substrait
=========

Java Substrait offer capabilities to ``Query`` data received as a Susbtrait
Plan (`plain or binary format`).

During this process, Substrait plans are read, executed, and ArrowReaders are
returned for reading Schema and ArrowRecordBatches. For Substrait plan that contains
``Local Files`` the URI per table are defined in the Substrait plan, different
than ``Named Tables`` where is needed to define a mapping name of tables
and theirs ArrowReader representation.

.. contents::

Getting Started
===============

Java Substrait API uses Acero C++ Substrait API capabilities thru JNI wrappers.

.. seealso:: :doc:`../cpp/streaming_execution` for more information on Acero.

Substrait Consumer
==================

Substrait Plan offer two ways to define URI for Query data:

- Local Files: A fixed URI value on the plan
- Named Table: An external configuration to define URI value

Local Files:

.. code-block:: json

    "local_files": {
      "items": [
        {
          "uri_file": "file:///tmp/opt/lineitem.parquet",
          "parquet": {}
        }
      ]
    }

Named Table:

.. code-block:: json

    "namedTable": {
        "names": ["LINEITEM"]
    }

Here is an example of a Java program that queries a Parquet file using Java Substrait:

.. code-block:: Java

    // Query: SELECT * from nation
    String uri = "file:///data/tpch_parquet/nation.parquet";
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, uri);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      // map table to reader
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", reader);
      // get binary plan
      String sql = "SELECT * from nation";
      String nation = "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), " +
          "N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))";
      Plan plan = getPlan(sql, ImmutableList.of(nation));
      ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.toByteArray().length);
      substraitPlan.put(plan.toByteArray());
      // run query
      try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQuery(
          substraitPlan,
          mapTableToArrowReader
      )) {
        while (arrowReader.loadNextBatch()) {
          assertEquals(arrowReader.getVectorSchemaRoot().getRowCount(), 25);
          assertTrue(arrowReader.getVectorSchemaRoot().contentToTSVString().contains("MOROCCO"));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

.. code-block:: text

    // Results example:
    FieldPath(0)	FieldPath(1)	FieldPath(2)	FieldPath(3)
    0	ALGERIA	0	 haggle. carefully final deposits detect slyly agai
    1	ARGENTINA	1	al foxes promise slyly according to the regular accounts. bold requests alon

Substrait Producer
==================

The following options are available for producing Substrait Plans: Acero,
Isthmus, Ibis, DuckDB, others.

You can generate Substrait plans and then send them to Java Substrait for consumption.
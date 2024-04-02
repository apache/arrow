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

The ``arrow-dataset`` module can execute Substrait_ plans via the :doc:`Acero <../cpp/streaming_execution>`
query engine.

.. contents::

Executing Queries Using Substrait Plans
=======================================

Plans can reference data in files via URIs, or "named tables" that must be provided along with the plan.

Here is an example of a Java program that queries a Parquet file using Java Substrait
(this example use `Substrait Java`_ project to compile a SQL query to a Substrait plan):

.. code-block:: Java

    import com.google.common.collect.ImmutableList;
    import io.substrait.isthmus.SqlToSubstrait;
    import io.substrait.proto.Plan;
    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.dataset.substrait.AceroSubstraitConsumer;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowReader;
    import org.apache.calcite.sql.parser.SqlParseException;

    import java.nio.ByteBuffer;
    import java.util.HashMap;
    import java.util.Map;

    public class ClientSubstrait {
        public static void main(String[] args) {
            String uri = "file:///data/tpch_parquet/nation.parquet";
            ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
            try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(),
                        FileFormat.PARQUET, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options);
                ArrowReader reader = scanner.scanBatches()
            ) {
                // map table to reader
                Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
                mapTableToArrowReader.put("NATION", reader);
                // get binary plan
                Plan plan = getPlan();
                ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.toByteArray().length);
                substraitPlan.put(plan.toByteArray());
                // run query
                try (ArrowReader arrowReader = new AceroSubstraitConsumer(allocator).runQuery(
                        substraitPlan,
                        mapTableToArrowReader
                )) {
                    while (arrowReader.loadNextBatch()) {
                        System.out.println(arrowReader.getVectorSchemaRoot().contentToTSVString());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        static Plan getPlan() throws SqlParseException {
            String sql = "SELECT * from nation";
            String nation = "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), " +
                    "N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))";
            SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
            Plan plan = sqlToSubstrait.execute(sql, ImmutableList.of(nation));
            return plan;
        }
    }

.. code-block:: text

    // Results example:
    FieldPath(0)	FieldPath(1)	FieldPath(2)	FieldPath(3)
    0	ALGERIA	0	 haggle. carefully final deposits detect slyly agai
    1	ARGENTINA	1	al foxes promise slyly according to the regular accounts. bold requests alon

Executing Projections and Filters Using Extended Expressions
============================================================

Dataset also supports projections and filters with Substrait's `Extended Expression`_.
This requires the substrait-java library.

This Java program:

- Loads a Parquet file containing the "nation" table from the TPC-H benchmark.
- Applies a filter:
    - ``N_NATIONKEY > 18``
- Projects two new columns:
    - ``N_REGIONKEY + 10``
    - ``N_NAME || ' - ' || N_COMMENT``



.. code-block:: Java

    import com.google.common.collect.ImmutableList;
    import io.substrait.isthmus.SqlExpressionToSubstrait;
    import io.substrait.proto.ExtendedExpression;
    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowReader;
    import org.apache.calcite.sql.parser.SqlParseException;

    import java.nio.ByteBuffer;
    import java.util.Base64;
    import java.util.Optional;

    public class ClientSubstraitExtendedExpressionsCookbook {

      public static void main(String[] args) throws SqlParseException {
        projectAndFilterDataset();
      }

      private static void projectAndFilterDataset() throws SqlParseException {
        String uri = "file:///Users/data/tpch_parquet/nation.parquet";
        ScanOptions options =
            new ScanOptions.Builder(/*batchSize*/ 32768)
                .columns(Optional.empty())
                .substraitFilter(getByteBuffer(new String[]{"N_NATIONKEY > 18"}))
                .substraitProjection(getByteBuffer(new String[]{"N_REGIONKEY + 10",
                    "N_NAME || CAST(' - ' as VARCHAR) || N_COMMENT"}))
                .build();
        try (BufferAllocator allocator = new RootAllocator();
             DatasetFactory datasetFactory =
                 new FileSystemDatasetFactory(
                     allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
             Dataset dataset = datasetFactory.finish();
             Scanner scanner = dataset.newScan(options);
             ArrowReader reader = scanner.scanBatches()) {
          while (reader.loadNextBatch()) {
            System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      private static ByteBuffer getByteBuffer(String[] sqlExpression) throws SqlParseException {
        String schema =
            "CREATE TABLE NATION (N_NATIONKEY INT NOT NULL, N_NAME VARCHAR, "
                + "N_REGIONKEY INT NOT NULL, N_COMMENT VARCHAR)";
        SqlExpressionToSubstrait expressionToSubstrait = new SqlExpressionToSubstrait();
        ExtendedExpression expression =
            expressionToSubstrait.convert(sqlExpression, ImmutableList.of(schema));
        byte[] expressionToByte =
            Base64.getDecoder().decode(Base64.getEncoder().encodeToString(expression.toByteArray()));
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(expressionToByte.length);
        byteBuffer.put(expressionToByte);
        return byteBuffer;
      }
    }

.. code-block:: text

    column-1	column-2
    13	ROMANIA - ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account
    14	SAUDI ARABIA - ts. silent requests haggle. closely express packages sleep across the blithely
    12	VIETNAM - hely enticingly express accounts. even, final
    13	RUSSIA -  requests against the platelets use never according to the quickly regular pint
    13	UNITED KINGDOM - eans boost carefully special requests. accounts are. carefull
    11	UNITED STATES - y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be

.. _`Substrait`: https://substrait.io/
.. _`Substrait Java`: https://github.com/substrait-io/substrait-java
.. _`Acero`: https://arrow.apache.org/docs/cpp/streaming_execution.html
.. _`Extended Expression`: https://github.com/substrait-io/substrait/blob/main/site/docs/expressions/extended_expression.md

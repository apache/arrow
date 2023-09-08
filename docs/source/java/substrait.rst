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

Executing Substrait Plans
=========================

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

.. _`Substrait`: https://substrait.io/
.. _`Substrait Java`: https://github.com/substrait-io/substrait-java
.. _`Acero`: https://arrow.apache.org/docs/cpp/streaming_execution.html
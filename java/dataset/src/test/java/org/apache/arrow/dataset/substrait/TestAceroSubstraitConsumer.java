/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.dataset.substrait;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.dataset.ParquetWriteSupport;
import org.apache.arrow.dataset.TestDataset;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAceroSubstraitConsumer extends TestDataset {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();
  public static final String AVRO_SCHEMA_USER = "user.avsc";

  @Test
  public void testRunQueryLocalFiles() throws Exception {
    //Query:
    //SELECT id, name FROM Users
    //Isthmus:
    //./isthmus-macOS-0.7.0  -c "CREATE TABLE USERS ( id INT NOT NULL, name VARCHAR(150));" "SELECT id, name FROM Users"
    //VARCHAR(150) -> is mapping to -> {ARROW:extension:name=varchar, ARROW:extension:metadata=varchar{length:150}}
    Map<String, String> metadataName = new HashMap<>();
    metadataName.put("ARROW:extension:name", "varchar");
    metadataName.put("ARROW:extension:metadata", "varchar{length:150}");
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("ID", new ArrowType.Int(32, true)),
        new Field("NAME", new FieldType(true, new ArrowType.Utf8(), null, metadataName), null)
    ), Collections.emptyMap());
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a", 11, "b", 21, "c");
    try (ArrowReader arrowReader = new AceroSubstraitConsumer(rootAllocator())
        .runQuery(
            new String(Files.readAllBytes(Paths.get(TestAceroSubstraitConsumer.class.getClassLoader()
                .getResource("substrait/local_files_users.json").toURI()))).replace("FILENAME_PLACEHOLDER",
                writeSupport.getOutputURI())
        )
    ) {
      assertEquals(schema, arrowReader.getVectorSchemaRoot().getSchema());
      int rowcount = 0;
      while (arrowReader.loadNextBatch()) {
        rowcount += arrowReader.getVectorSchemaRoot().getRowCount();
      }
      assertEquals(3, rowcount);
    }
  }

  @Test
  public void testRunQueryNamedTable() throws Exception {
    //Query:
    //SELECT id, name FROM Users
    //Isthmus:
    //./isthmus-macOS-0.7.0  -c "CREATE TABLE USERS ( id INT NOT NULL, name VARCHAR(150));" "SELECT id, name FROM Users"
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("ID", new ArrowType.Int(32, true)),
        Field.nullable("NAME", new ArrowType.Utf8())
    ), Collections.emptyMap());
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a", 11, "b", 21, "c");
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("USERS", reader);
      try (ArrowReader arrowReader = new AceroSubstraitConsumer(rootAllocator()).runQuery(
          new String(Files.readAllBytes(Paths.get(TestAceroSubstraitConsumer.class.getClassLoader()
              .getResource("substrait/named_table_users.json").toURI()))),
          mapTableToArrowReader
      )) {
        assertEquals(schema, arrowReader.getVectorSchemaRoot().getSchema());
        assertEquals(arrowReader.getVectorSchemaRoot().getSchema(), schema);
        int rowcount = 0;
        while (arrowReader.loadNextBatch()) {
          rowcount += arrowReader.getVectorSchemaRoot().getRowCount();
        }
        assertEquals(3, rowcount);
      }
    }
  }

  @Test(expected = RuntimeException.class)
  public void testRunQueryNamedTableWithException() throws Exception {
    //Query:
    //SELECT id, name FROM Users
    //Isthmus:
    //./isthmus-macOS-0.7.0  -c "CREATE TABLE USERS ( id INT NOT NULL, name VARCHAR(150));" "SELECT id, name FROM Users"
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("ID", new ArrowType.Int(32, true)),
        Field.nullable("NAME", new ArrowType.Utf8())
    ), Collections.emptyMap());
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a", 11, "b", 21, "c");
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("USERS_INVALID_MAP", reader);
      try (ArrowReader arrowReader = new AceroSubstraitConsumer(rootAllocator()).runQuery(
          new String(Files.readAllBytes(Paths.get(TestAceroSubstraitConsumer.class.getClassLoader()
              .getResource("substrait/named_table_users.json").toURI()))),
          mapTableToArrowReader
      )) {
        assertEquals(schema, arrowReader.getVectorSchemaRoot().getSchema());
        int rowcount = 0;
        while (arrowReader.loadNextBatch()) {
          rowcount += arrowReader.getVectorSchemaRoot().getRowCount();
        }
        assertEquals(3, rowcount);
      }
    }
  }

  @Test
  public void testRunBinaryQueryNamedTable() throws Exception {
    //Query:
    //SELECT id, name FROM Users
    //Isthmus:
    //./isthmus-macOS-0.7.0  -c "CREATE TABLE USERS ( id INT NOT NULL, name VARCHAR(150));" "SELECT id, name FROM Users"
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("ID", new ArrowType.Int(32, true)),
        Field.nullable("NAME", new ArrowType.Utf8())
    ), Collections.emptyMap());
    // Base64.getEncoder().encodeToString(plan.toByteArray());
    String binaryPlan =
        "Gl8SXQpROk8KBhIECgICAxIvCi0KAgoAEh4KAklECgROQU1FEhIKBCoCEAEKC" +
            "LIBBQiWARgBGAI6BwoFVVNFUlMaCBIGCgISACIAGgoSCAoEEgIIASIAEgJJRBIETkFNRQ==";
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a", 11, "b", 21, "c");
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      // map table to reader
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("USERS", reader);
      // get binary plan
      byte[] plan = Base64.getDecoder().decode(binaryPlan);
      ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.length);
      substraitPlan.put(plan);
      // run query
      try (ArrowReader arrowReader = new AceroSubstraitConsumer(rootAllocator()).runQuery(
          substraitPlan,
          mapTableToArrowReader
      )) {
        assertEquals(schema, arrowReader.getVectorSchemaRoot().getSchema());
        int rowcount = 0;
        while (arrowReader.loadNextBatch()) {
          rowcount += arrowReader.getVectorSchemaRoot().getRowCount();
        }
        assertEquals(3, rowcount);
      }
    }
  }

  @Test
  public void testBaseParquetReadWithExtendedExpressionsProjectAndFilter() throws Exception {
    // Extended Expression 01 (`add` `2` to column `id`): id + 2
    // Extended Expression 02 (`concatenate` column `name` || column `name`): name || name
    // Extended Expression 03 (`filter` 'id' < 20): id < 20
    // Extended expression result: [add_two_to_column_a, add(FieldPath(0), 2),
    // concat_column_a_and_b, binary_join_element_wise(FieldPath(1), FieldPath(1), ""),
    // filter_one, (FieldPath(0) < 20)]
    // Base64.getEncoder().encodeToString(plan.toByteArray()): Generated throughout Substrait POJO Extended Expressions
    String binaryExtendedExpressions = "Ch4IARIaL2Z1bmN0aW9uc19hcml0aG1ldGljLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlz" +
        "b24ueWFtbBIRGg8IARoLYWRkOmkzMl9pMzISFBoSCAIQARoMY29uY2F0OnZjaGFyEhIaEAgCEAIaCmx0OmFueV9hbnkaMQoaGhgaBCoCEAE" +
        "iCBoGEgQKAhIAIgYaBAoCKAIaE2FkZF90d29fdG9fY29sdW1uX2EaOwoiGiAIARoEYgIQASIKGggSBgoEEgIIASIKGggSBgoEEgIIARoVY2" +
        "9uY2F0X2NvbHVtbl9hX2FuZF9iGjcKHBoaCAIaBAoCEAEiCBoGEgQKAhIAIgYaBAoCKBQaF2ZpbHRlcl9pZF9sb3dlcl90aGFuXzIwIhoKA" +
        "klECgROQU1FEg4KBCoCEAEKBGICEAEYAg==";
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("add_two_to_column_a", new ArrowType.Int(32, true)),
        Field.nullable("concat_column_a_and_b", new ArrowType.Utf8())
    ), null);
    byte[] extendedExpressions = Base64.getDecoder().decode(binaryExtendedExpressions);
    ByteBuffer substraitExtendedExpressions = ByteBuffer.allocateDirect(extendedExpressions.length);
    substraitExtendedExpressions.put(extendedExpressions);
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 19, "value_19", 1, "value_1",
            11, "value_11", 21, "value_21", 45, "value_45");
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768, substraitExtendedExpressions);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newSubstraitScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      assertEquals(schema.getFields(), reader.getVectorSchemaRoot().getSchema().getFields());
      int rowcount = 0;
      while (reader.loadNextBatch()) {
        rowcount += reader.getVectorSchemaRoot().getRowCount();
      }
      assertEquals(3, rowcount);
    }
  }

  @Test(expected = RuntimeException.class)
  public void testBaseParquetReadWithExtendedExpressionsProjectAndFilterException() throws Exception {
    // Extended Expression 01 (`add` `2` to column `id`): id + 2
    // Extended Expression 02 (`concatenate` column `name` || column `name`): name || name
    // Extended Expression 03 (`filter` 'id' < 20): id < 20
    // Extended Expression 04 (`filter` 'id' < 20): id < 20
    // Extended expression result: [add_two_to_column_a, add(FieldPath(0), 2), concat_column_a_and_b,
    // binary_join_element_wise(FieldPath(1), FieldPath(1), ""), filter_id_lower_than_20, (FieldPath(0) < 20),
    // filter_id_lower_than_10, (FieldPath(0) < 10)]
    // Base64.getEncoder().encodeToString(plan.toByteArray()): Generated throughout Substrait POJO Extended Expressions
    String binaryExtendedExpressions = "Ch4IARIaL2Z1bmN0aW9uc19hcml0aG1ldGljLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlz" +
        "b24ueWFtbBIRGg8IARoLYWRkOmkzMl9pMzISFBoSCAIQARoMY29uY2F0OnZjaGFyEhIaEAgCEAIaCmx0OmFueV9hbnkSEhoQCAIQAhoKbHQ" +
        "6YW55X2FueRoxChoaGBoEKgIQASIIGgYSBAoCEgAiBhoECgIoAhoTYWRkX3R3b190b19jb2x1bW5fYRo7CiIaIAgBGgRiAhABIgoaCBIGCg" +
        "QSAggBIgoaCBIGCgQSAggBGhVjb25jYXRfY29sdW1uX2FfYW5kX2IaNwocGhoIAhoECgIQASIIGgYSBAoCEgAiBhoECgIoFBoXZmlsdGVyX" +
        "2lkX2xvd2VyX3RoYW5fMjAaNwocGhoIAhoECgIQASIIGgYSBAoCEgAiBhoECgIoChoXZmlsdGVyX2lkX2xvd2VyX3RoYW5fMTAiGgoCSUQK" +
        "BE5BTUUSDgoEKgIQAQoEYgIQARgC";
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("add_two_to_column_a", new ArrowType.Int(32, true)),
        Field.nullable("concat_column_a_and_b", new ArrowType.Utf8())
    ), null);
    byte[] extendedExpressions = Base64.getDecoder().decode(binaryExtendedExpressions);
    ByteBuffer substraitExtendedExpressions = ByteBuffer.allocateDirect(extendedExpressions.length);
    substraitExtendedExpressions.put(extendedExpressions);
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 19, "value_19", 1, "value_1", 11,
            "value_11", 21, "value_21", 45, "value_45");
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768, substraitExtendedExpressions);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newSubstraitScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      assertEquals(schema.getFields(), reader.getVectorSchemaRoot().getSchema().getFields());
      int rowcount = 0;
      while (reader.loadNextBatch()) {
        rowcount += reader.getVectorSchemaRoot().getRowCount();
      }
      assertEquals(3, rowcount);
    } catch (Exception e) {
      assertEquals("Only one filter expression may be provided", e.getMessage());
      throw e;
    }
  }
}

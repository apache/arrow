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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
      ByteBuffer substraitPlan = getByteBuffer(binaryPlan);
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
  public void testRunExtendedExpressionsFilter() throws Exception {
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("id", new ArrowType.Int(32, true)),
        Field.nullable("name", new ArrowType.Utf8())
    ), null);
    // Substrait Extended Expression: Filter:
    // Expression 01: WHERE ID < 20
    String base64EncodedSubstraitFilter = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEhoQCAIQAhoKbHQ6YW55X2F" +
        "ueRo3ChwaGggCGgQKAhABIggaBhIECgISACIGGgQKAigUGhdmaWx0ZXJfaWRfbG93ZXJfdGhhbl8yMCIaCgJJRAoETkFNRRIOCgQqAhA" +
        "BCgRiAhABGAI=";
    ByteBuffer substraitExpressionFilter = getByteBuffer(base64EncodedSubstraitFilter);
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 19, "value_19", 1, "value_1",
            11, "value_11", 21, "value_21", 45, "value_45");
    ScanOptions options = new ScanOptions.Builder(/*batchSize*/ 32768)
        .columns(Optional.empty())
        .substraitFilter(substraitExpressionFilter)
        .build();
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      assertEquals(schema.getFields(), reader.getVectorSchemaRoot().getSchema().getFields());
      int rowcount = 0;
      while (reader.loadNextBatch()) {
        rowcount += reader.getVectorSchemaRoot().getRowCount();
        assertTrue(reader.getVectorSchemaRoot().getVector("id").toString().equals("[19, 1, 11]"));
        assertTrue(reader.getVectorSchemaRoot().getVector("name").toString()
            .equals("[value_19, value_1, value_11]"));
      }
      assertEquals(3, rowcount);
    }
  }

  @Test
  public void testRunExtendedExpressionsFilterWithProjectionsInsteadOfFilterException() throws Exception {
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("id", new ArrowType.Int(32, true)),
        Field.nullable("name", new ArrowType.Utf8())
    ), null);
    // Substrait Extended Expression: Project New Column:
    // Expression ADD: id + 2
    // Expression CONCAT: name + '-' + name
    String base64EncodedSubstraitFilter = "Ch4IARIaL2Z1bmN0aW9uc19hcml0aG1ldGljLnlhbWwSERoPCAEaC2FkZDppM" +
        "zJfaTMyEhQaEggCEAEaDGNvbmNhdDp2Y2hhchoxChoaGBoEKgIQASIIGgYSBAoCEgAiBhoECgIoAhoTYWRkX3R3b190b19jb2x1" +
        "bW5fYRpGCi0aKwgBGgRiAhABIgoaCBIGCgQSAggBIgkaBwoFYgMgLSAiChoIEgYKBBICCAEaFWNvbmNhdF9jb2x1bW5fYV9hbmR" +
        "fYiIaCgJJRAoETkFNRRIOCgQqAhABCgRiAhABGAI=";
    ByteBuffer substraitExpressionFilter = getByteBuffer(base64EncodedSubstraitFilter);
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 19, "value_19", 1, "value_1",
            11, "value_11", 21, "value_21", 45, "value_45");
    ScanOptions options = new ScanOptions.Builder(/*batchSize*/ 32768)
        .columns(Optional.empty())
        .substraitFilter(substraitExpressionFilter)
        .build();
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
        Dataset dataset = datasetFactory.finish()
    ) {
      Exception e = assertThrows(RuntimeException.class, () -> dataset.newScan(options));
      assertTrue(e.getMessage().startsWith("There is no filter expression in the expression provided"));
    }
  }

  @Test
  public void testRunExtendedExpressionsFilterWithEmptyFilterException() throws Exception {
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("id", new ArrowType.Int(32, true)),
        Field.nullable("name", new ArrowType.Utf8())
    ), null);
    String base64EncodedSubstraitFilter = "";
    ByteBuffer substraitExpressionFilter = getByteBuffer(base64EncodedSubstraitFilter);
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 19, "value_19", 1, "value_1",
            11, "value_11", 21, "value_21", 45, "value_45");
    ScanOptions options = new ScanOptions.Builder(/*batchSize*/ 32768)
        .columns(Optional.empty())
        .substraitFilter(substraitExpressionFilter)
        .build();
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
        Dataset dataset = datasetFactory.finish()
    ) {
      Exception e = assertThrows(RuntimeException.class, () -> dataset.newScan(options));
      assertTrue(e.getMessage().contains("no anonymous struct type was provided to which names could be attached."));
    }
  }

  @Test
  public void testRunExtendedExpressionsProjection() throws Exception {
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("add_two_to_column_a", new ArrowType.Int(32, true)),
        Field.nullable("concat_column_a_and_b", new ArrowType.Utf8())
    ), null);
    // Substrait Extended Expression: Project New Column:
    // Expression ADD: id + 2
    // Expression CONCAT: name + '-' + name
    String binarySubstraitExpressionProject = "Ch4IARIaL2Z1bmN0aW9uc19hcml0aG1ldGljLnlhbWwSERoPCAEaC2FkZDppM" +
        "zJfaTMyEhQaEggCEAEaDGNvbmNhdDp2Y2hhchoxChoaGBoEKgIQASIIGgYSBAoCEgAiBhoECgIoAhoTYWRkX3R3b190b19jb2x1" +
        "bW5fYRpGCi0aKwgBGgRiAhABIgoaCBIGCgQSAggBIgkaBwoFYgMgLSAiChoIEgYKBBICCAEaFWNvbmNhdF9jb2x1bW5fYV9hbmR" +
        "fYiIaCgJJRAoETkFNRRIOCgQqAhABCgRiAhABGAI=";
    ByteBuffer substraitExpressionProject = getByteBuffer(binarySubstraitExpressionProject);
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 19, "value_19", 1, "value_1",
            11, "value_11", 21, "value_21", 45, "value_45");
    ScanOptions options = new ScanOptions.Builder(/*batchSize*/ 32768)
        .columns(Optional.empty())
         .substraitProjection(substraitExpressionProject)
        .build();
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      assertEquals(schema.getFields(), reader.getVectorSchemaRoot().getSchema().getFields());
      int rowcount = 0;
      while (reader.loadNextBatch()) {
        assertTrue(reader.getVectorSchemaRoot().getVector("add_two_to_column_a").toString()
            .equals("[21, 3, 13, 23, 47]"));
        assertTrue(reader.getVectorSchemaRoot().getVector("concat_column_a_and_b").toString()
            .equals("[value_19 - value_19, value_1 - value_1, value_11 - value_11, " +
                "value_21 - value_21, value_45 - value_45]"));
        rowcount += reader.getVectorSchemaRoot().getRowCount();
      }
      assertEquals(5, rowcount);
    }
  }

  @Test
  public void testRunExtendedExpressionsProjectionWithFilterInsteadOfProjectionException() throws Exception {
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("filter_id_lower_than_20", new ArrowType.Bool())
    ), null);
    // Substrait Extended Expression: Filter:
    // Expression 01: WHERE ID < 20
    String binarySubstraitExpressionFilter = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEhoQCAIQAhoKbHQ6YW55X2F" +
        "ueRo3ChwaGggCGgQKAhABIggaBhIECgISACIGGgQKAigUGhdmaWx0ZXJfaWRfbG93ZXJfdGhhbl8yMCIaCgJJRAoETkFNRRIOCgQqAhA" +
        "BCgRiAhABGAI=";
    ByteBuffer substraitExpressionFilter = getByteBuffer(binarySubstraitExpressionFilter);
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 19, "value_19", 1, "value_1",
            11, "value_11", 21, "value_21", 45, "value_45");
    ScanOptions options = new ScanOptions.Builder(/*batchSize*/ 32768)
        .columns(Optional.empty())
        .substraitProjection(substraitExpressionFilter)
        .build();
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      assertEquals(schema.getFields(), reader.getVectorSchemaRoot().getSchema().getFields());
      int rowcount = 0;
      while (reader.loadNextBatch()) {
        assertTrue(reader.getVectorSchemaRoot().getVector("filter_id_lower_than_20").toString()
            .equals("[true, true, true, false, false]"));
        rowcount += reader.getVectorSchemaRoot().getRowCount();
      }
      assertEquals(5, rowcount);
    }
  }

  @Test
  public void testRunExtendedExpressionsProjectionWithEmptyProjectionException() throws Exception {
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("id", new ArrowType.Int(32, true)),
        Field.nullable("name", new ArrowType.Utf8())
    ), null);
    String base64EncodedSubstraitFilter = "";
    ByteBuffer substraitExpressionProjection = getByteBuffer(base64EncodedSubstraitFilter);
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 19, "value_19", 1, "value_1",
            11, "value_11", 21, "value_21", 45, "value_45");
    ScanOptions options = new ScanOptions.Builder(/*batchSize*/ 32768)
        .columns(Optional.empty())
        .substraitProjection(substraitExpressionProjection)
        .build();
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
        Dataset dataset = datasetFactory.finish()
    ) {
      Exception e = assertThrows(RuntimeException.class, () -> dataset.newScan(options));
      assertTrue(e.getMessage().contains("no anonymous struct type was provided to which names could be attached."));
    }
  }

  @Test
  public void testRunExtendedExpressionsProjectAndFilter() throws Exception {
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("add_two_to_column_a", new ArrowType.Int(32, true)),
        Field.nullable("concat_column_a_and_b", new ArrowType.Utf8())
    ), null);
    // Substrait Extended Expression: Project New Column:
    // Expression ADD: id + 2
    // Expression CONCAT: name + '-' + name
    String binarySubstraitExpressionProject = "Ch4IARIaL2Z1bmN0aW9uc19hcml0aG1ldGljLnlhbWwSERoPCAEaC2FkZDppM" +
        "zJfaTMyEhQaEggCEAEaDGNvbmNhdDp2Y2hhchoxChoaGBoEKgIQASIIGgYSBAoCEgAiBhoECgIoAhoTYWRkX3R3b190b19jb2x1" +
        "bW5fYRpGCi0aKwgBGgRiAhABIgoaCBIGCgQSAggBIgkaBwoFYgMgLSAiChoIEgYKBBICCAEaFWNvbmNhdF9jb2x1bW5fYV9hbmR" +
        "fYiIaCgJJRAoETkFNRRIOCgQqAhABCgRiAhABGAI=";
    ByteBuffer substraitExpressionProject = getByteBuffer(binarySubstraitExpressionProject);
    // Substrait Extended Expression: Filter:
    // Expression 01: WHERE ID < 20
    String base64EncodedSubstraitFilter = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEhoQCAIQAhoKbHQ6YW55X2F" +
        "ueRo3ChwaGggCGgQKAhABIggaBhIECgISACIGGgQKAigUGhdmaWx0ZXJfaWRfbG93ZXJfdGhhbl8yMCIaCgJJRAoETkFNRRIOCgQqAhA" +
        "BCgRiAhABGAI=";
    ByteBuffer substraitExpressionFilter = getByteBuffer(base64EncodedSubstraitFilter);
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 19, "value_19", 1, "value_1",
            11, "value_11", 21, "value_21", 45, "value_45");
    ScanOptions options = new ScanOptions.Builder(/*batchSize*/ 32768)
        .columns(Optional.empty())
        .substraitProjection(substraitExpressionProject)
        .substraitFilter(substraitExpressionFilter)
        .build();
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      assertEquals(schema.getFields(), reader.getVectorSchemaRoot().getSchema().getFields());
      int rowcount = 0;
      while (reader.loadNextBatch()) {
        assertTrue(reader.getVectorSchemaRoot().getVector("add_two_to_column_a").toString()
            .equals("[21, 3, 13]"));
        assertTrue(reader.getVectorSchemaRoot().getVector("concat_column_a_and_b").toString()
            .equals("[value_19 - value_19, value_1 - value_1, value_11 - value_11]"));
        rowcount += reader.getVectorSchemaRoot().getRowCount();
      }
      assertEquals(3, rowcount);
    }
  }

  private static ByteBuffer getByteBuffer(String base64EncodedSubstrait) {
    byte[] decodedSubstrait = Base64.getDecoder().decode(base64EncodedSubstrait);
    ByteBuffer substraitExpression = ByteBuffer.allocateDirect(decodedSubstrait.length);
    substraitExpression.put(decodedSubstrait);
    return substraitExpression;
  }
}

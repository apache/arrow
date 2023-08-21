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
  public void testRunQueryNamedTableNation() throws Exception {
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
  public void testRunQueryNamedTableNationWithException() throws Exception {
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
  public void testRunBinaryQueryNamedTableNation() throws Exception {
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
}

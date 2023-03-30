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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAceroSubstraitConsumer extends TestDataset {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();
  public static final String AVRO_SCHEMA_USER = "user.avsc";
  private RootAllocator allocator = null;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  protected RootAllocator rootAllocator() {
    return allocator;
  }

  @Test
  public void testRunQueryLocalFiles() throws Exception {
    //Query:
    //SELECT id, name FROM Users
    //Isthmus:
    //./isthmus-macOS-0.7.0  -c "CREATE TABLE USERS ( id INT NOT NULL, name VARCHAR(150));" "SELECT id, name FROM Users"
    ParquetWriteSupport writeSupport = ParquetWriteSupport
        .writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a", 11, "b", 21, "c");
    try (ArrowReader arrowReader = new AceroSubstraitConsumer(rootAllocator())
        .runQuery(
            planReplaceLocalFileURI(
                getSubstraitPlan("local_files_users.json"),
                writeSupport.getOutputURI()
            ),
            Collections.EMPTY_MAP
        )
    ) {
      while (arrowReader.loadNextBatch()) {
        assertEquals(3, arrowReader.getVectorSchemaRoot().getRowCount());
        assertEquals(2, arrowReader.getVectorSchemaRoot().getSchema().getFields().size());
        assertEquals("ID", arrowReader.getVectorSchemaRoot().getSchema().getFields().get(0).getName());
        assertEquals("NAME", arrowReader.getVectorSchemaRoot().getSchema().getFields().get(1).getName());
        assertEquals(Types.MinorType.INT.getType(),
            arrowReader.getVectorSchemaRoot().getSchema().getFields().get(0).getType());
        assertEquals(Types.MinorType.VARCHAR.getType(),
            arrowReader.getVectorSchemaRoot().getSchema().getFields().get(1).getType());
      }
    }
  }

  @Test
  public void testRunQueryNamedTableNation() throws Exception {
    //Query:
    //SELECT id, name FROM Users
    //Isthmus:
    //./isthmus-macOS-0.7.0  -c "CREATE TABLE USERS ( id INT NOT NULL, name VARCHAR(150));" "SELECT id, name FROM Users"
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
          getSubstraitPlan("named_table_users.json"),
          mapTableToArrowReader
      )) {
        while (arrowReader.loadNextBatch()) {
          assertEquals(3, arrowReader.getVectorSchemaRoot().getRowCount());
          assertEquals(2, arrowReader.getVectorSchemaRoot().getSchema().getFields().size());
          assertEquals("ID", arrowReader.getVectorSchemaRoot().getSchema().getFields().get(0).getName());
          assertEquals("NAME", arrowReader.getVectorSchemaRoot().getSchema().getFields().get(1).getName());
          assertEquals(Types.MinorType.INT.getType(),
              arrowReader.getVectorSchemaRoot().getSchema().getFields().get(0).getType());
          assertEquals(Types.MinorType.VARCHAR.getType(),
              arrowReader.getVectorSchemaRoot().getSchema().getFields().get(1).getType());
        }
      }
    }
  }

  @Test
  public void testRunBinaryQueryNamedTableNation() throws Exception {
    //Query:
    //SELECT id, name FROM Users
    //Isthmus:
    //./isthmus-macOS-0.7.0  -c "CREATE TABLE USERS ( id INT NOT NULL, name VARCHAR(150));" "SELECT id, name FROM Users"
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
      byte[] plan = getBinarySubstraitPlan("named_table_users.binary");
      ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.length);
      substraitPlan.put(plan);
      // run query
      try (ArrowReader arrowReader = new AceroSubstraitConsumer(rootAllocator()).runQuery(
          substraitPlan,
          mapTableToArrowReader
      )) {
        while (arrowReader.loadNextBatch()) {
          assertEquals(3, arrowReader.getVectorSchemaRoot().getRowCount());
          assertEquals(2, arrowReader.getVectorSchemaRoot().getSchema().getFields().size());
          assertEquals("ID", arrowReader.getVectorSchemaRoot().getSchema().getFields().get(0).getName());
          assertEquals("NAME", arrowReader.getVectorSchemaRoot().getSchema().getFields().get(1).getName());
          assertEquals(Types.MinorType.INT.getType(),
              arrowReader.getVectorSchemaRoot().getSchema().getFields().get(0).getType());
          assertEquals(Types.MinorType.VARCHAR.getType(),
              arrowReader.getVectorSchemaRoot().getSchema().getFields().get(1).getType());
        }
      }
    }
  }

  private static String planReplaceLocalFileURI(String plan, String uri) {
    StringBuilder builder = new StringBuilder(plan);
    builder.replace(builder.indexOf("FILENAME_PLACEHOLDER"),
        builder.indexOf("FILENAME_PLACEHOLDER") + "FILENAME_PLACEHOLDER".length(), uri);
    return builder.toString();
  }

  protected String getSubstraitPlan(String name) throws IOException {
    return new String(
        Files.readAllBytes(
            Paths.get(
                Paths.get(
                        Paths.get("").toAbsolutePath().toString(),
                        "src", "test", "resources", "substrait", name
                    )
                    .toFile()
                    .getAbsolutePath()
            )
        )
    );
  }

  protected byte[] getBinarySubstraitPlan(String name) throws IOException {
    return Files.readAllBytes(
        Paths.get(
            Paths.get(
                Paths.get("").toAbsolutePath().toString(), "src", "test", "resources", "substrait", name)
                .toFile()
                .getAbsolutePath()
        )
    );
  }
}

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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSubstraitConsumer extends TestDataset {
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
    try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator())
        .runQueryLocalFiles(
            planReplaceLocalFileURI(
                getSubstraitPlan("local_files_binary.json"),
                getNamedTableUri("binary.parquet")
            )
        )
    ) {
      while (arrowReader.loadNextBatch()) {
        assertEquals(arrowReader.getVectorSchemaRoot().getRowCount(), 12);
      }
    }
  }

  @Test
  public void testRunQueryNamedTableNation() throws Exception {
    // Query: SELECT * from nation
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, getNamedTableUri("nation.parquet"));
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", reader);
      try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
          getSubstraitPlan("named_table_nation.json"),
          mapTableToArrowReader
      )) {
        while (arrowReader.loadNextBatch()) {
          assertEquals(arrowReader.getVectorSchemaRoot().getRowCount(), 25);
          assertTrue(arrowReader.getVectorSchemaRoot().contentToTSVString().contains("MOROCCO"));
        }
      }
    }
  }

  @Test
  public void testRunQueryNamedTableNationAndCustomer() throws Exception {
    // Query:
    // SELECT n.n_name, c.c_name, c.c_phone, c.c_address FROM nation n JOIN customer c ON n.n_nationkey = c.c_nationkey
    ScanOptions optionsNations = new ScanOptions(/*batchSize*/ 32768);
    ScanOptions optionsCustomer = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, getNamedTableUri("nation.parquet"));
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(optionsNations);
        ArrowReader readerNation = scanner.scanBatches();
        DatasetFactory datasetFactoryCustomer = new FileSystemDatasetFactory(rootAllocator(),
            NativeMemoryPool.getDefault(), FileFormat.PARQUET, getNamedTableUri("customer.parquet"));
        Dataset datasetCustomer = datasetFactoryCustomer.finish();
        Scanner scannerCustomer = datasetCustomer.newScan(optionsCustomer);
        ArrowReader readerCustomer = scannerCustomer.scanBatches()
    ) {
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", readerNation);
      mapTableToArrowReader.put("CUSTOMER", readerCustomer);
      try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
          getSubstraitPlan("named_table_nation_customer.json"),
          mapTableToArrowReader
      )) {
        while (arrowReader.loadNextBatch()) {
          assertEquals(arrowReader.getVectorSchemaRoot().getRowCount(), 15000);
          assertTrue(arrowReader.getVectorSchemaRoot().contentToTSVString().contains("Customer#000014924"));
        }
      }
    }
  }

  @Test
  public void testRunBinaryQueryNamedTableNation() throws Exception {
    // Query: SELECT * from nation
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, getNamedTableUri("nation.parquet"));
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      // map table to reader
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", reader);
      // get binary plan
      byte[] plan = getBinarySubstraitPlan("named_table_nation.binary");
      ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.length);
      substraitPlan.put(plan);
      // run query
      try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
          substraitPlan,
          mapTableToArrowReader
      )) {
        while (arrowReader.loadNextBatch()) {
          assertEquals(arrowReader.getVectorSchemaRoot().getRowCount(), 25);
          assertTrue(arrowReader.getVectorSchemaRoot().contentToTSVString().contains("MOROCCO"));
        }
      }
    }
  }

  @Test
  public void testRunBinaryQueryNamedTableNationAndCustomer() throws Exception {
    // Query:
    // SELECT n.n_name, c.c_name, c.c_phone, c.c_address FROM nation n JOIN customer c ON n.n_nationkey = c.c_nationkey
    ScanOptions optionsNations = new ScanOptions(/*batchSize*/ 32768);
    ScanOptions optionsCustomer = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, getNamedTableUri("nation.parquet"));
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(optionsNations);
        ArrowReader readerNation = scanner.scanBatches();
        DatasetFactory datasetFactoryCustomer = new FileSystemDatasetFactory(rootAllocator(),
            NativeMemoryPool.getDefault(), FileFormat.PARQUET, getNamedTableUri("customer.parquet"));
        Dataset datasetCustomer = datasetFactoryCustomer.finish();
        Scanner scannerCustomer = datasetCustomer.newScan(optionsCustomer);
        ArrowReader readerCustomer = scannerCustomer.scanBatches()
    ) {
      // map table to reader
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", readerNation);
      mapTableToArrowReader.put("CUSTOMER", readerCustomer);
      // get binary plan
      byte[] plan = getBinarySubstraitPlan("named_table_nation_customer.binary");
      ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.length);
      substraitPlan.put(plan);
      // run query
      try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
          substraitPlan,
          mapTableToArrowReader
      )) {
        while (arrowReader.loadNextBatch()) {
          assertEquals(arrowReader.getVectorSchemaRoot().getRowCount(), 15000);
          assertTrue(arrowReader.getVectorSchemaRoot().contentToTSVString().contains("Customer#000014924"));
        }
      }
    }
  }

  private static String planReplaceLocalFileURI(String plan, String uri) throws IOException {
    StringBuilder builder = new StringBuilder(plan);
    builder.replace(builder.indexOf("FILENAME_PLACEHOLDER"),
        builder.indexOf("FILENAME_PLACEHOLDER") + "FILENAME_PLACEHOLDER".length(), uri);
    return builder.toString();
  }
}

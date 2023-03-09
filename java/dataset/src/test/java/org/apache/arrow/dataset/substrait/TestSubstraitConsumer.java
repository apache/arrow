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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

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

public class TestSubstraitConsumer {
  private RootAllocator allocator = null;

  public static String planReplaceLocalFileURI(String plan, String uri) throws IOException {
    StringBuilder builder = new StringBuilder(plan);
    builder.replace(builder.indexOf("FILENAME_PLACEHOLDER"),
        builder.indexOf("FILENAME_PLACEHOLDER") + "FILENAME_PLACEHOLDER".length(), uri);
    return builder.toString();
  }

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
    String uri = Paths.get(
        Paths.get("src", "test", "resources", "substrait", "parquet", "binary.parquet")
            .toFile()
            .getAbsolutePath()
    ).toUri().toString();
    try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator())
        .runQueryLocalFiles(
            planReplaceLocalFileURI(
                new String(
                    Files.readAllBytes(
                        Paths.get(
                            Paths.get("src", "test", "resources", "substrait", "plan", "local_files_binary.json")
                                .toFile()
                                .getAbsolutePath()
                        )
                    )
                ),
                uri
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
    String uri = Paths.get(
        Paths.get("src", "test", "resources", "substrait", "parquet", "nation.parquet")
            .toFile()
            .getAbsolutePath()
    ).toUri().toString();
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, uri);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      Map<String, ArrowReader> mapReaderToTable = new HashMap<>();
      mapReaderToTable.put("NATION", reader);
      try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
          new String(
              Files.readAllBytes(
                  Paths.get(
                      Paths.get("src", "test", "resources", "substrait", "plan", "named_table_nation.json")
                          .toFile()
                          .getAbsolutePath()
                  )
              )
          ),
          mapReaderToTable
      )) {
        while (arrowReader.loadNextBatch()) {
          assertEquals(arrowReader.getVectorSchemaRoot().getRowCount(), 25);
          assertTrue(arrowReader.getVectorSchemaRoot().contentToTSVString().contains("MOROCCO"));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testRunQueryNamedTableNationAndCustomer() throws Exception {
    // Query:
    // SELECT n.n_name, c.c_name, c.c_phone, c.c_address FROM nation n JOIN customer c ON n.n_nationkey = c.c_nationkey
    String uriNation = Paths.get(
        Paths.get("src", "test", "resources", "substrait", "parquet", "nation.parquet")
            .toFile()
            .getAbsolutePath()
    ).toUri().toString();
    String uriCustomer = Paths.get(
        Paths.get("src", "test", "resources", "substrait", "parquet", "customer.parquet")
            .toFile()
            .getAbsolutePath()
    ).toUri().toString();
    ScanOptions optionsNations = new ScanOptions(/*batchSize*/ 32768);
    ScanOptions optionsCustomer = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, uriNation);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(optionsNations);
        ArrowReader readerNation = scanner.scanBatches();
        DatasetFactory datasetFactoryCustomer = new FileSystemDatasetFactory(rootAllocator(),
            NativeMemoryPool.getDefault(), FileFormat.PARQUET, uriCustomer);
        Dataset datasetCustomer = datasetFactoryCustomer.finish();
        Scanner scannerCustomer = datasetCustomer.newScan(optionsCustomer);
        ArrowReader readerCustomer = scannerCustomer.scanBatches()
    ) {
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", readerNation);
      mapTableToArrowReader.put("CUSTOMER", readerCustomer);
      try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
          new String(
              Files.readAllBytes(
                  Paths.get(
                      Paths.get("src", "test", "resources", "substrait", "plan", "named_table_nation_customer.json")
                          .toFile()
                          .getAbsolutePath()
                  )
              )
          ),
          mapTableToArrowReader
      )) {
        while (arrowReader.loadNextBatch()) {
          assertEquals(arrowReader.getVectorSchemaRoot().getRowCount(), 15000);
          assertTrue(arrowReader.getVectorSchemaRoot().contentToTSVString().contains("Customer#000014924"));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

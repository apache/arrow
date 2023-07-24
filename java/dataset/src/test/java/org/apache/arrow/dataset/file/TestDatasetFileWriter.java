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

package org.apache.arrow.dataset.file;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.ParquetWriteSupport;
import org.apache.arrow.dataset.TestDataset;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ArrowScannerReader;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestDatasetFileWriter extends TestDataset {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  public static final String AVRO_SCHEMA_USER = "user.avsc";

  @Test
  public void testParquetWriteSimple() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(),
        1, "a", 2, "b", 3, "c", 2, "d");
    String sampleParquet = writeSupport.getOutputURI();
    ScanOptions options = new ScanOptions(new String[0], 100);
    final File writtenFolder = TMP.newFolder();
    final String writtenParquet = writtenFolder.toURI().toString();
    try (FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, sampleParquet);
         final Dataset dataset = factory.finish();
         final Scanner scanner = dataset.newScan(options);
         final ArrowScannerReader reader = new ArrowScannerReader(scanner, rootAllocator());
    ) {
      DatasetFileWriter.write(rootAllocator(), reader, FileFormat.PARQUET, writtenParquet);
      assertParquetFileEquals(sampleParquet, Objects.requireNonNull(writtenFolder.listFiles())[0].toURI().toString());
    }
  }

  @Test
  public void testParquetWriteWithPartitions() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(),
        1, "a", 2, "b", 3, "c", 2, "d");
    String sampleParquet = writeSupport.getOutputURI();
    ScanOptions options = new ScanOptions(new String[0], 100);
    final File writtenFolder = TMP.newFolder();
    final String writtenParquet = writtenFolder.toURI().toString();

    try (FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, sampleParquet);
         final Dataset dataset = factory.finish();
         final Scanner scanner = dataset.newScan(options);
         final ArrowScannerReader reader = new ArrowScannerReader(scanner, rootAllocator());
    ) {
      DatasetFileWriter.write(rootAllocator(), reader,
          FileFormat.PARQUET, writtenParquet, new String[]{"id", "name"},
          100, "data_{i}");
      final Set<String> expectedOutputFiles = new HashSet<>(
          Arrays.asList("id=1/name=a/data_0", "id=2/name=b/data_0", "id=3/name=c/data_0", "id=2/name=d/data_0"));
      final Set<String> outputFiles = FileUtils.listFiles(writtenFolder, null, true)
          .stream()
          .map(file -> {
            return writtenFolder.toURI().relativize(file.toURI()).toString();
          })
          .collect(Collectors.toSet());
      Assert.assertEquals(expectedOutputFiles, outputFiles);
    }
  }

  private void assertParquetFileEquals(String expectedURI, String actualURI) throws Exception {
    final FileSystemDatasetFactory expectedFactory = new FileSystemDatasetFactory(
        rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, expectedURI);
    final FileSystemDatasetFactory actualFactory = new FileSystemDatasetFactory(
        rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, actualURI);
    List<ArrowRecordBatch> expectedBatches = collectResultFromFactory(expectedFactory,
        new ScanOptions(new String[0], 100));
    List<ArrowRecordBatch> actualBatches = collectResultFromFactory(actualFactory,
        new ScanOptions(new String[0], 100));
    try (
        VectorSchemaRoot expectVsr = VectorSchemaRoot.create(expectedFactory.inspect(), rootAllocator());
        VectorSchemaRoot actualVsr = VectorSchemaRoot.create(actualFactory.inspect(), rootAllocator())) {

      // fast-fail by comparing metadata
      Assert.assertEquals(expectedBatches.toString(), actualBatches.toString());
      // compare ArrowRecordBatches
      Assert.assertEquals(expectedBatches.size(), actualBatches.size());
      VectorLoader expectLoader = new VectorLoader(expectVsr);
      VectorLoader actualLoader = new VectorLoader(actualVsr);
      for (int i = 0; i < expectedBatches.size(); i++) {
        expectLoader.load(expectedBatches.get(i));
        actualLoader.load(actualBatches.get(i));
        for (int j = 0; j < expectVsr.getFieldVectors().size(); j++) {
          FieldVector vector = expectVsr.getFieldVectors().get(i);
          FieldVector otherVector = actualVsr.getFieldVectors().get(i);
          // TODO: ARROW-18140 Use VectorSchemaRoot#equals() method to compare
          Assert.assertTrue(VectorEqualsVisitor.vectorEquals(vector, otherVector));
        }
      }
    } finally {
      AutoCloseables.close(expectedBatches, actualBatches);
    }
  }
}


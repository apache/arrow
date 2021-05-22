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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.ParquetWriteSupport;
import org.apache.arrow.dataset.TestDataset;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
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
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, sampleParquet);
    ScanOptions options = new ScanOptions(new String[0], 100);
    final Dataset dataset = factory.finish();
    final Scanner scanner = dataset.newScan(options);
    final File writtenFolder = TMP.newFolder();
    final String writtenParquet = writtenFolder.toURI().toString();
    try {
      DatasetFileWriter.write(scanner, FileFormat.PARQUET, writtenParquet);
      assertParquetFileEquals(sampleParquet, Objects.requireNonNull(writtenFolder.listFiles())[0].toURI().toString());
    } finally {
      AutoCloseables.close(factory, scanner, dataset);
    }
  }

  @Test
  public void testParquetWriteWithPartitions() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(),
        1, "a", 2, "b", 3, "c", 2, "d");
    String sampleParquet = writeSupport.getOutputURI();
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, sampleParquet);
    ScanOptions options = new ScanOptions(new String[0], 100);
    final Dataset dataset = factory.finish();
    final Scanner scanner = dataset.newScan(options);
    final File writtenFolder = TMP.newFolder();
    final String writtenParquet = writtenFolder.toURI().toString();
    try {
      DatasetFileWriter.write(scanner, FileFormat.PARQUET, writtenParquet, new String[]{"id", "name"}, 100, "dat_{i}");
      final Set<String> expectedOutputFiles = new HashSet<>(
          Arrays.asList("id=1/name=a/dat_0", "id=2/name=b/dat_1", "id=3/name=c/dat_2", "id=2/name=d/dat_3"));
      final Set<String> outputFiles = FileUtils.listFiles(writtenFolder, null, true)
          .stream()
          .map(file -> {
            return writtenFolder.toURI().relativize(file.toURI()).toString();
          })
          .collect(Collectors.toSet());
      Assert.assertEquals(expectedOutputFiles, outputFiles);
    } finally {
      AutoCloseables.close(factory, scanner, dataset);
    }
  }

  @Test(expected = java.lang.RuntimeException.class)
  public void testScanErrorHandling() throws Exception {
    DatasetFileWriter.write(new Scanner() {
      @Override
      public Iterable<? extends ScanTask> scan() {
        return Collections.singletonList(new ScanTask() {
          @Override
          public BatchIterator execute() {
            // this error is supposed to be firstly investigated in native code, then thrown back to Java.
            throw new RuntimeException("ERROR");
          }

          @Override
          public void close() throws Exception {
            // do nothing
          }
        });
      }

      @Override
      public Schema schema() {
        return new Schema(Collections.emptyList());
      }

      @Override
      public void close() throws Exception {
        // do nothing
      }

    }, FileFormat.PARQUET, "file:/DUMMY/");
  }

  private void assertParquetFileEquals(String expectedURI, String actualURI) throws Exception {
    final FileSystemDatasetFactory expectedFactory = new FileSystemDatasetFactory(
        rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, expectedURI);
    List<ArrowRecordBatch> expectedBatches = collectResultFromFactory(expectedFactory,
        new ScanOptions(new String[0], 100));
    final FileSystemDatasetFactory actualFactory = new FileSystemDatasetFactory(
        rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, actualURI);
    List<ArrowRecordBatch> actualBatches = collectResultFromFactory(actualFactory,
        new ScanOptions(new String[0], 100));
    // fast-fail by comparing metadata
    Assert.assertEquals(expectedBatches.toString(), actualBatches.toString());
    // compare buffers
    Assert.assertEquals(serialize(expectedBatches), serialize(actualBatches));
    AutoCloseables.close(expectedBatches, actualBatches);
  }

  private String serialize(List<ArrowRecordBatch> batches) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (ArrowRecordBatch batch : batches) {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch);
    }
    return Arrays.toString(out.toByteArray());
  }
}

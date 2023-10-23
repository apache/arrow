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

package org.apache.arrow.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;


public abstract class TestDataset {
  private BufferAllocator allocator = null;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  protected BufferAllocator rootAllocator() {
    return allocator;
  }

  protected List<ArrowRecordBatch> collectResultFromFactory(DatasetFactory factory, ScanOptions options) {
    final Dataset dataset = factory.finish();
    final Scanner scanner = dataset.newScan(options);
    try {
      final List<ArrowRecordBatch> ret = collectTaskData(scanner);
      AutoCloseables.close(scanner, dataset);
      return ret;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected List<ArrowRecordBatch> collectTaskData(Scanner scan) {
    try (ArrowReader reader = scan.scanBatches()) {
      List<ArrowRecordBatch> batches = new ArrayList<>();
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        final VectorUnloader unloader = new VectorUnloader(root);
        batches.add(unloader.getRecordBatch());
      }
      return batches;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected Schema inferResultSchemaFromFactory(DatasetFactory factory, ScanOptions options) {
    final Dataset dataset = factory.finish();
    final Scanner scanner = dataset.newScan(options);
    final Schema schema = scanner.schema();
    try {
      AutoCloseables.close(scanner, dataset);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return schema;
  }

  protected void assertParquetFileEquals(String expectedURI, String actualURI) throws Exception {
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

  protected <T> Stream<T> stream(Iterable<T> iterable) {
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  protected <T> List<T> collect(Iterable<T> iterable) {
    return stream(iterable).collect(Collectors.toList());
  }

  protected <T> Stream<T> stream(Iterator<T> iterator) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
  }

  protected <T> List<T> collect(Iterator<T> iterator) {
    return stream(iterator).collect(Collectors.toList());
  }
}

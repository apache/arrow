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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.ParquetWriteSupport;
import org.apache.arrow.dataset.jni.NativeDataset;
import org.apache.arrow.dataset.jni.NativeInstanceReleasedException;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.jni.NativeScanTask;
import org.apache.arrow.dataset.jni.NativeScanner;
import org.apache.arrow.dataset.jni.TestNativeDataset;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;

public class TestFileSystemDataset extends TestNativeDataset {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  public static final String AVRO_SCHEMA_USER = "user.avsc";

  @Test
  public void testBaseParquetRead() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    ScanOptions options = new ScanOptions(100);
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);

    assertSingleTaskProduced(factory, options);
    assertEquals(1, datum.size());
    assertEquals(2, schema.getFields().size());
    assertEquals("id", schema.getFields().get(0).getName());
    assertEquals("name", schema.getFields().get(1).getName());
    assertEquals(Types.MinorType.INT.getType(), schema.getFields().get(0).getType());
    assertEquals(Types.MinorType.VARCHAR.getType(), schema.getFields().get(1).getType());
    checkParquetReadResult(schema, writeSupport.getWrittenRecords(), datum);

    AutoCloseables.close(datum);
  }

  @Test
  public void testParquetProjectSingleColumn() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    ScanOptions options = new ScanOptions(100, Optional.of(new String[]{"id"}));
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    org.apache.avro.Schema expectedSchema = truncateAvroSchema(writeSupport.getAvroSchema(), 0, 1);

    assertSingleTaskProduced(factory, options);
    assertEquals(1, schema.getFields().size());
    assertEquals("id", schema.getFields().get(0).getName());
    assertEquals(Types.MinorType.INT.getType(), schema.getFields().get(0).getType());
    assertEquals(1, datum.size());
    checkParquetReadResult(schema,
        Collections.singletonList(
            new GenericRecordBuilder(
                expectedSchema)
                .set("id", 1)
                .build()), datum);

    AutoCloseables.close(datum);
  }

  @Test
  public void testParquetBatchSize() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(),
        1, "a", 2, "b", 3, "c");

    ScanOptions options = new ScanOptions(1);
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);

    assertSingleTaskProduced(factory, options);
    assertEquals(3, datum.size());
    datum.forEach(batch -> assertEquals(1, batch.getLength()));
    checkParquetReadResult(schema, writeSupport.getWrittenRecords(), datum);

    AutoCloseables.close(datum);
  }

  @Test
  public void testEmptyProjectSelectsZeroColumns() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    ScanOptions options = new ScanOptions(100, Optional.of(new String[0]));
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    org.apache.avro.Schema expectedSchema = org.apache.avro.Schema.createRecord(Collections.emptyList());

    assertSingleTaskProduced(factory, options);
    assertEquals(0, schema.getFields().size());
    assertEquals(1, datum.size());
    checkParquetReadResult(schema,
        Collections.singletonList(
            new GenericRecordBuilder(
                expectedSchema)
                .build()), datum);

    AutoCloseables.close(datum);
  }

  @Test
  public void testNullProjectSelectsAllColumns() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    ScanOptions options = new ScanOptions(100, Optional.empty());
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);

    assertSingleTaskProduced(factory, options);
    assertEquals(1, datum.size());
    assertEquals(2, schema.getFields().size());
    assertEquals("id", schema.getFields().get(0).getName());
    assertEquals("name", schema.getFields().get(1).getName());
    assertEquals(Types.MinorType.INT.getType(), schema.getFields().get(0).getType());
    assertEquals(Types.MinorType.VARCHAR.getType(), schema.getFields().get(1).getType());
    checkParquetReadResult(schema, writeSupport.getWrittenRecords(), datum);

    AutoCloseables.close(datum);
  }

  @Test
  public void testNoErrorWhenCloseAgain() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());

    assertDoesNotThrow(() -> {
      NativeDataset dataset = factory.finish();
      dataset.close();
      dataset.close();
    });
  }

  @Test
  public void testErrorThrownWhenScanAgain() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(100);
    NativeScanner scanner = dataset.newScan(options);
    List<? extends NativeScanTask> taskList1 = collect(scanner.scan());
    List<? extends NativeScanTask> taskList2 = collect(scanner.scan());
    NativeScanTask task1 = taskList1.get(0);
    NativeScanTask task2 = taskList2.get(0);
    List<ArrowRecordBatch> datum = collect(task1.execute());

    UnsupportedOperationException uoe = assertThrows(UnsupportedOperationException.class, task2::execute);
    Assertions.assertEquals("NativeScanner cannot be executed more than once. Consider creating new scanner instead",
        uoe.getMessage());

    AutoCloseables.close(datum);
    AutoCloseables.close(taskList1);
    AutoCloseables.close(taskList2);
    AutoCloseables.close(scanner, dataset, factory);
  }

  @Test
  public void testScanInOtherThread() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(100);
    NativeScanner scanner = dataset.newScan(options);
    List<? extends NativeScanTask> taskList = collect(scanner.scan());
    NativeScanTask task = taskList.get(0);
    List<ArrowRecordBatch> datum = executor.submit(() -> collect(task.execute())).get();

    AutoCloseables.close(datum);
    AutoCloseables.close(taskList);
    AutoCloseables.close(scanner, dataset, factory);
  }

  @Test
  public void testErrorThrownWhenScanAfterScannerClose() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(100);
    NativeScanner scanner = dataset.newScan(options);
    scanner.close();
    assertThrows(NativeInstanceReleasedException.class, scanner::scan);
  }

  @Test
  public void testErrorThrownWhenExecuteTaskAfterTaskClose() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(100);
    NativeScanner scanner = dataset.newScan(options);
    List<? extends NativeScanTask> tasks = collect(scanner.scan());
    NativeScanTask task = tasks.get(0);
    task.close();
    assertThrows(NativeInstanceReleasedException.class, task::execute);
  }

  @Test
  public void testErrorThrownWhenIterateOnIteratorAfterTaskClose() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(100);
    NativeScanner scanner = dataset.newScan(options);
    List<? extends NativeScanTask> tasks = collect(scanner.scan());
    NativeScanTask task = tasks.get(0);
    ScanTask.BatchIterator iterator = task.execute();
    task.close();
    assertThrows(NativeInstanceReleasedException.class, iterator::hasNext);
  }

  @Test
  public void testMemoryAllocationOnAssociatedAllocator() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
    ScanOptions options = new ScanOptions(100);
    long initReservation = rootAllocator().getAllocatedMemory();
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    final long expected_diff = datum.stream()
            .flatMapToLong(batch -> batch.getBuffers()
                    .stream()
                    .mapToLong(buf -> buf.getReferenceManager().getAccountedSize())).sum();
    long reservation = rootAllocator().getAllocatedMemory();
    AutoCloseables.close(datum);
    long finalReservation = rootAllocator().getAllocatedMemory();
    Assert.assertEquals(expected_diff, reservation - initReservation);
    Assert.assertEquals(-expected_diff, finalReservation - reservation);
  }

  private void checkParquetReadResult(Schema schema, List<GenericRecord> expected, List<ArrowRecordBatch> actual) {
    assertEquals(expected.size(), actual.stream()
        .mapToInt(ArrowRecordBatch::getLength)
        .sum());
    final int fieldCount = schema.getFields().size();
    LinkedList<GenericRecord> expectedRemovable = new LinkedList<>(expected);
    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, rootAllocator())) {
      VectorLoader loader = new VectorLoader(vsr);
      for (ArrowRecordBatch batch : actual) {
        try {
          assertEquals(fieldCount, batch.getNodes().size());
          loader.load(batch);
          int batchRowCount = vsr.getRowCount();
          for (int i = 0; i < fieldCount; i++) {
            FieldVector vector = vsr.getVector(i);
            for (int j = 0; j < batchRowCount; j++) {
              Object object = vector.getObject(j);
              Object expectedObject = expectedRemovable.get(j).get(i);
              assertEquals(Objects.toString(expectedObject),
                  Objects.toString(object));
            }
          }
          for (int i = 0; i < batchRowCount; i++) {
            expectedRemovable.poll();
          }
        } finally {
          batch.close();
        }
      }
      assertTrue(expectedRemovable.isEmpty());
    }
  }

  private org.apache.avro.Schema truncateAvroSchema(org.apache.avro.Schema schema, int from, int to) {
    List<org.apache.avro.Schema.Field> fields = schema.getFields().subList(from, to);
    return org.apache.avro.Schema.createRecord(
        fields.stream()
            .map(f -> new org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()))
            .collect(Collectors.toList()));
  }
}

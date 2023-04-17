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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.CsvWriteSupport;
import org.apache.arrow.dataset.OrcWriteSupport;
import org.apache.arrow.dataset.ParquetWriteSupport;
import org.apache.arrow.dataset.jni.NativeDataset;
import org.apache.arrow.dataset.jni.NativeInstanceReleasedException;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.jni.NativeScanner;
import org.apache.arrow.dataset.jni.TestNativeDataset;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Primitives;

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

    assertScanBatchesProduced(factory, options);
    assertEquals(1, datum.size());
    assertEquals(2, schema.getFields().size());
    assertEquals("id", schema.getFields().get(0).getName());
    assertEquals("name", schema.getFields().get(1).getName());
    assertEquals(Types.MinorType.INT.getType(), schema.getFields().get(0).getType());
    assertEquals(Types.MinorType.VARCHAR.getType(), schema.getFields().get(1).getType());
    checkParquetReadResult(schema, writeSupport.getWrittenRecords(), datum);

    AutoCloseables.close(datum);
    AutoCloseables.close(factory);
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

    assertScanBatchesProduced(factory, options);
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
    AutoCloseables.close(factory);
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

    assertScanBatchesProduced(factory, options);
    assertEquals(3, datum.size());
    datum.forEach(batch -> assertEquals(1, batch.getLength()));
    checkParquetReadResult(schema, writeSupport.getWrittenRecords(), datum);

    AutoCloseables.close(datum);
    AutoCloseables.close(factory);
  }

  @Test
  public void testParquetDirectoryRead() throws Exception {
    final File outputFolder = TMP.newFolder();
    ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, outputFolder,
        1, "a", 2, "b", 3, "c");
    ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, outputFolder,
        4, "e", 5, "f", 6, "g", 7, "h");
    String expectedJsonUnordered = "[[1,\"a\"],[2,\"b\"],[3,\"c\"],[4,\"e\"],[5,\"f\"],[6,\"g\"],[7,\"h\"]]";

    ScanOptions options = new ScanOptions(new String[0], 1);
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, outputFolder.toURI().toString());
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);

    assertScanBatchesProduced(factory, options);
    assertEquals(7, datum.size());
    datum.forEach(batch -> assertEquals(1, batch.getLength()));
    checkParquetReadResult(schema, expectedJsonUnordered, datum);

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

    assertScanBatchesProduced(factory, options);
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

    assertScanBatchesProduced(factory, options);
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

    AutoCloseables.close(factory);
  }

  @Test
  public void testErrorThrownWhenScanBatchesAgain() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(100);
    NativeScanner scanner = dataset.newScan(options);
    List<ArrowRecordBatch> datum = collectTaskData(scanner);
    AutoCloseables.close(datum);
    UnsupportedOperationException uoe = assertThrows(UnsupportedOperationException.class,
            scanner::scanBatches);
    Assertions.assertEquals("NativeScanner can only be executed once. Create a new scanner instead",
        uoe.getMessage());

    AutoCloseables.close(scanner, dataset, factory);
  }

  @Test
  public void testScanBatchesInOtherThread() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(100);
    NativeScanner scanner = dataset.newScan(options);
    List<ArrowRecordBatch> datum = executor.submit(() -> collectTaskData(scanner)).get();

    AutoCloseables.close(datum);
    AutoCloseables.close(scanner, dataset, factory);
  }

  @Test
  public void testErrorThrownWhenScanBatchesAfterScannerClose() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(100);
    NativeScanner scanner = dataset.newScan(options);
    scanner.close();
    assertThrows(NativeInstanceReleasedException.class, scanner::scanBatches);

    AutoCloseables.close(factory);
  }

  @Test
  public void testErrorThrownWhenReadAfterNativeReaderClose() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(100);
    NativeScanner scanner = dataset.newScan(options);
    ArrowReader reader = scanner.scanBatches();
    scanner.close();
    assertThrows(NativeInstanceReleasedException.class, reader::loadNextBatch);

    AutoCloseables.close(factory);
  }

  @Test
  public void testBaseArrowIpcRead() throws Exception {
    File dataFile = TMP.newFile();
    Schema sourceSchema = new Schema(Collections.singletonList(Field.nullable("ints", new ArrowType.Int(32, true))));
    try (VectorSchemaRoot root = VectorSchemaRoot.create(sourceSchema, rootAllocator());
         FileOutputStream sink = new FileOutputStream(dataFile);
         ArrowFileWriter writer = new ArrowFileWriter(root, /*dictionaryProvider=*/null, sink.getChannel())) {
      IntVector ints = (IntVector) root.getVector(0);
      ints.setSafe(0, 0);
      ints.setSafe(1, 1024);
      ints.setSafe(2, Integer.MAX_VALUE);
      root.setRowCount(3);
      writer.start();
      writer.writeBatch();
      writer.end();
    }

    String arrowDataURI = dataFile.toURI().toString();
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.ARROW_IPC, arrowDataURI);
    ScanOptions options = new ScanOptions(100);
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);

    assertScanBatchesProduced(factory, options);
    assertEquals(1, datum.size());
    assertEquals(1, schema.getFields().size());
    assertEquals("ints", schema.getFields().get(0).getName());

    String expectedJsonUnordered = String.format("[[0],[1024],[%d]]", Integer.MAX_VALUE);
    checkParquetReadResult(schema, expectedJsonUnordered, datum);

    AutoCloseables.close(datum);
    AutoCloseables.close(factory);
  }

  @Test
  public void testBaseOrcRead() throws Exception {
    String dataName = "test-orc";
    String basePath = TMP.getRoot().getAbsolutePath();

    TypeDescription orcSchema = TypeDescription.fromString("struct<ints:int>");
    Path path = new Path(basePath, dataName);
    OrcWriteSupport.writeTempFile(orcSchema, path, new Integer[]{Integer.MIN_VALUE, Integer.MAX_VALUE});

    String orcDatasetUri = new File(basePath, dataName).toURI().toString();
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.ORC, orcDatasetUri);
    ScanOptions options = new ScanOptions(100);
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);

    assertScanBatchesProduced(factory, options);
    assertEquals(1, datum.size());
    assertEquals(1, schema.getFields().size());
    assertEquals("ints", schema.getFields().get(0).getName());

    String expectedJsonUnordered = "[[2147483647], [-2147483648]]";
    checkParquetReadResult(schema, expectedJsonUnordered, datum);

    AutoCloseables.close(datum);
    AutoCloseables.close(factory);
  }

  @Test
  public void testBaseCsvRead() throws Exception {
    CsvWriteSupport writeSupport = CsvWriteSupport.writeTempFile(
            TMP.newFolder(), "Name,Language", "Juno,Java", "Peter,Python", "Celin,C++");
    String expectedJsonUnordered = "[[\"Juno\", \"Java\"], [\"Peter\", \"Python\"], [\"Celin\", \"C++\"]]";
    ScanOptions options = new ScanOptions(100);
    try (
        FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.CSV, writeSupport.getOutputURI())
    ) {
      List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
      Schema schema = inferResultSchemaFromFactory(factory, options);

      assertScanBatchesProduced(factory, options);
      assertEquals(1, datum.size());
      assertEquals(2, schema.getFields().size());
      assertEquals("Name", schema.getFields().get(0).getName());

      checkParquetReadResult(schema, expectedJsonUnordered, datum);

      AutoCloseables.close(datum);
    }
  }

  private void checkParquetReadResult(Schema schema, String expectedJson, List<ArrowRecordBatch> actual)
      throws IOException {
    final ObjectMapper json = new ObjectMapper();
    final Set<?> expectedSet = json.readValue(expectedJson, Set.class);
    final Set<List<Object>> actualSet = new HashSet<>();
    final int fieldCount = schema.getFields().size();
    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, rootAllocator())) {
      VectorLoader loader = new VectorLoader(vsr);
      for (ArrowRecordBatch batch : actual) {
        loader.load(batch);
        int batchRowCount = vsr.getRowCount();
        for (int i = 0; i < batchRowCount; i++) {
          List<Object> row = new ArrayList<>();
          for (int j = 0; j < fieldCount; j++) {
            Object object = vsr.getVector(j).getObject(i);
            if (Primitives.isWrapperType(object.getClass())) {
              row.add(object);
            } else {
              row.add(object.toString());
            }
          }
          actualSet.add(row);
        }
      }
    }
    Assert.assertEquals("Mismatched data read from Parquet, actual: " + json.writeValueAsString(actualSet) + ";",
        expectedSet, actualSet);
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

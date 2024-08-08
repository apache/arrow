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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.scanner.csv.CsvConvertOptions;
import org.apache.arrow.dataset.scanner.csv.CsvFragmentScanOptions;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueIterableVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.jupiter.api.Test;

public class TestFragmentScanOptions {

  @Test
  public void testCsvConvertOptions() throws Exception {
    final Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("Id", new ArrowType.Int(32, true)),
                Field.nullable("Name", new ArrowType.Utf8()),
                Field.nullable("Language", new ArrowType.Utf8())),
            null);
    String path = "file://" + getClass().getResource("/").getPath() + "/data/student.csv";
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try (ArrowSchema cSchema = ArrowSchema.allocateNew(allocator);
        CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
      Data.exportSchema(allocator, schema, provider, cSchema);
      CsvConvertOptions convertOptions = new CsvConvertOptions(ImmutableMap.of("delimiter", ";"));
      convertOptions.setArrowSchema(cSchema);
      CsvFragmentScanOptions fragmentScanOptions =
          new CsvFragmentScanOptions(convertOptions, ImmutableMap.of(), ImmutableMap.of());
      ScanOptions options =
          new ScanOptions.Builder(/*batchSize*/ 32768)
              .columns(Optional.empty())
              .fragmentScanOptions(fragmentScanOptions)
              .build();
      try (DatasetFactory datasetFactory =
              new FileSystemDatasetFactory(
                  allocator, NativeMemoryPool.getDefault(), FileFormat.CSV, path);
          Dataset dataset = datasetFactory.finish();
          Scanner scanner = dataset.newScan(options);
          ArrowReader reader = scanner.scanBatches()) {

        assertEquals(schema.getFields(), reader.getVectorSchemaRoot().getSchema().getFields());
        int rowCount = 0;
        while (reader.loadNextBatch()) {
          final ValueIterableVector<Integer> idVector =
              (ValueIterableVector<Integer>) reader.getVectorSchemaRoot().getVector("Id");
          assertThat(idVector.getValueIterable(), IsIterableContainingInOrder.contains(1, 2, 3));
          rowCount += reader.getVectorSchemaRoot().getRowCount();
        }
        assertEquals(3, rowCount);
      }
    }
  }

  @Test
  public void testCsvConvertOptionsDelimiterNotSet() throws Exception {
    final Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("Id", new ArrowType.Int(32, true)),
                Field.nullable("Name", new ArrowType.Utf8()),
                Field.nullable("Language", new ArrowType.Utf8())),
            null);
    String path = "file://" + getClass().getResource("/").getPath() + "/data/student.csv";
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try (ArrowSchema cSchema = ArrowSchema.allocateNew(allocator);
        CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
      Data.exportSchema(allocator, schema, provider, cSchema);
      CsvConvertOptions convertOptions = new CsvConvertOptions(ImmutableMap.of());
      convertOptions.setArrowSchema(cSchema);
      CsvFragmentScanOptions fragmentScanOptions =
          new CsvFragmentScanOptions(convertOptions, ImmutableMap.of(), ImmutableMap.of());
      ScanOptions options =
          new ScanOptions.Builder(/*batchSize*/ 32768)
              .columns(Optional.empty())
              .fragmentScanOptions(fragmentScanOptions)
              .build();
      try (DatasetFactory datasetFactory =
              new FileSystemDatasetFactory(
                  allocator, NativeMemoryPool.getDefault(), FileFormat.CSV, path);
          Dataset dataset = datasetFactory.finish();
          Scanner scanner = dataset.newScan(options);
          ArrowReader reader = scanner.scanBatches()) {

        assertEquals(schema.getFields(), reader.getVectorSchemaRoot().getSchema().getFields());
        int rowCount = 0;
        while (reader.loadNextBatch()) {
          final ValueIterableVector<Integer> idVector =
              (ValueIterableVector<Integer>) reader.getVectorSchemaRoot().getVector("Id");
          assertThat(idVector.getValueIterable(), IsIterableContainingInOrder.contains(1, 2, 3));
          rowCount += reader.getVectorSchemaRoot().getRowCount();
        }
        assertEquals(3, rowCount);
      }
    }
  }

  @Test
  public void testCsvConvertOptionsNoOption() throws Exception {
    final Schema schema =
        new Schema(
            Collections.singletonList(Field.nullable("Id;Name;Language", new ArrowType.Utf8())),
            null);
    String path = "file://" + getClass().getResource("/").getPath() + "/data/student.csv";
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ScanOptions options =
        new ScanOptions.Builder(/*batchSize*/ 32768).columns(Optional.empty()).build();
    try (DatasetFactory datasetFactory =
            new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.CSV, path);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()) {

      assertEquals(schema.getFields(), reader.getVectorSchemaRoot().getSchema().getFields());
      int rowCount = 0;
      while (reader.loadNextBatch()) {
        final ValueIterableVector<String> idVector =
            (ValueIterableVector<String>)
                reader.getVectorSchemaRoot().getVector("Id;Name;Language");
        assertThat(
            idVector.getValueIterable(),
            IsIterableContainingInOrder.contains(
                "1;Juno;Java\n" + "2;Peter;Python\n" + "3;Celin;C++"));
        rowCount += reader.getVectorSchemaRoot().getRowCount();
      }
      assertEquals(3, rowCount);
    }
  }

  @Test
  public void testCsvReadParseAndReadOptions() throws Exception {
    final Schema schema =
        new Schema(
            Collections.singletonList(Field.nullable("Id;Name;Language", new ArrowType.Utf8())),
            null);
    String path = "file://" + getClass().getResource("/").getPath() + "/data/student.csv";
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    CsvFragmentScanOptions fragmentScanOptions =
        new CsvFragmentScanOptions(
            new CsvConvertOptions(ImmutableMap.of()),
            ImmutableMap.of("skip_rows_after_names", "1"),
            ImmutableMap.of("delimiter", ";"));
    ScanOptions options =
        new ScanOptions.Builder(/*batchSize*/ 32768)
            .columns(Optional.empty())
            .fragmentScanOptions(fragmentScanOptions)
            .build();
    try (DatasetFactory datasetFactory =
            new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.CSV,
                path,
                Optional.of(fragmentScanOptions));
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()) {

      assertEquals(schema.getFields(), reader.getVectorSchemaRoot().getSchema().getFields());
      int rowCount = 0;
      while (reader.loadNextBatch()) {
        final ValueIterableVector<Text> idVector =
            (ValueIterableVector<Text>) reader.getVectorSchemaRoot().getVector("Id;Name;Language");
        assertThat(
            idVector.getValueIterable(),
            IsIterableContainingInOrder.contains(
                new Text("2;Peter;Python"), new Text("3;Celin;C++")));
        rowCount += reader.getVectorSchemaRoot().getRowCount();
      }
      assertEquals(2, rowCount);
    }
  }

  @Test
  public void testCsvReadOtherOptions() throws Exception {
    String path = "file://" + getClass().getResource("/").getPath() + "/data/student.csv";
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    Map<String, String> convertOption =
        ImmutableMap.of(
            "check_utf8",
            "true",
            "null_values",
            "NULL",
            "true_values",
            "True",
            "false_values",
            "False",
            "quoted_strings_can_be_null",
            "true",
            "auto_dict_encode",
            "false",
            "auto_dict_max_cardinality",
            "3456",
            "decimal_point",
            ".",
            "include_missing_columns",
            "false");
    Map<String, String> readOption =
        ImmutableMap.of(
            "use_threads",
            "true",
            "block_size",
            "1024",
            "skip_rows",
            "12",
            "skip_rows_after_names",
            "12",
            "autogenerate_column_names",
            "false");
    Map<String, String> parseOption =
        ImmutableMap.of(
            "delimiter",
            ".",
            "quoting",
            "true",
            "quote_char",
            "'",
            "double_quote",
            "False",
            "escaping",
            "true",
            "escape_char",
            "v",
            "newlines_in_values",
            "false",
            "ignore_empty_lines",
            "true");
    CsvFragmentScanOptions fragmentScanOptions =
        new CsvFragmentScanOptions(new CsvConvertOptions(convertOption), readOption, parseOption);
    ScanOptions options =
        new ScanOptions.Builder(/*batchSize*/ 32768)
            .columns(Optional.empty())
            .fragmentScanOptions(fragmentScanOptions)
            .build();
    try (DatasetFactory datasetFactory =
            new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.CSV, path);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options)) {
      assertNotNull(scanner);
    }
  }

  @Test
  public void testCsvInvalidOption() throws Exception {
    String path = "file://" + getClass().getResource("/").getPath() + "/data/student.csv";
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    Map<String, String> convertOption = ImmutableMap.of("not_exists_key_check_utf8", "true");
    CsvFragmentScanOptions fragmentScanOptions =
        new CsvFragmentScanOptions(
            new CsvConvertOptions(convertOption), ImmutableMap.of(), ImmutableMap.of());
    ScanOptions options =
        new ScanOptions.Builder(/*batchSize*/ 32768)
            .columns(Optional.empty())
            .fragmentScanOptions(fragmentScanOptions)
            .build();
    try (DatasetFactory datasetFactory =
            new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.CSV, path);
        Dataset dataset = datasetFactory.finish()) {
      assertThrows(RuntimeException.class, () -> dataset.newScan(options));
    }

    CsvFragmentScanOptions fragmentScanOptionsFaultValue =
        new CsvFragmentScanOptions(
            new CsvConvertOptions(ImmutableMap.of()),
            ImmutableMap.of("", ""),
            ImmutableMap.of("escape_char", "vbvb"));
    ScanOptions optionsFault =
        new ScanOptions.Builder(/*batchSize*/ 32768)
            .columns(Optional.empty())
            .fragmentScanOptions(fragmentScanOptionsFaultValue)
            .build();
    try (DatasetFactory datasetFactory =
            new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.CSV, path);
        Dataset dataset = datasetFactory.finish()) {
      assertThrows(RuntimeException.class, () -> dataset.newScan(optionsFault));
    }
  }
}

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

package org.apache.arrow.c;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class StreamTest {
  private RootAllocator allocator = null;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testRoundtripInts() throws Exception {
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("ints", new ArrowType.Int(32, true))));
    final List<ArrowRecordBatch> batches = new ArrayList<>();
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final IntVector ints = (IntVector) root.getVector(0);
      VectorUnloader unloader = new VectorUnloader(root);

      root.allocateNew();
      ints.setSafe(0, 1);
      ints.setSafe(1, 2);
      ints.setSafe(2, 4);
      ints.setSafe(3, 8);
      root.setRowCount(4);
      batches.add(unloader.getRecordBatch());

      root.allocateNew();
      ints.setSafe(0, 1);
      ints.setNull(1);
      ints.setSafe(2, 4);
      ints.setNull(3);
      root.setRowCount(4);
      batches.add(unloader.getRecordBatch());
      roundtrip(schema, batches);
    }
  }

  @Test
  public void roundtripStrings() throws Exception {
    final Schema schema = new Schema(Arrays.asList(Field.nullable("ints", new ArrowType.Int(32, true)),
        Field.nullable("strs", new ArrowType.Utf8())));
    final List<ArrowRecordBatch> batches = new ArrayList<>();
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final IntVector ints = (IntVector) root.getVector(0);
      final VarCharVector strs = (VarCharVector) root.getVector(1);
      VectorUnloader unloader = new VectorUnloader(root);

      root.allocateNew();
      ints.setSafe(0, 1);
      ints.setSafe(1, 2);
      ints.setSafe(2, 4);
      ints.setSafe(3, 8);
      strs.setSafe(0, "".getBytes(StandardCharsets.UTF_8));
      strs.setSafe(1, "a".getBytes(StandardCharsets.UTF_8));
      strs.setSafe(2, "bc".getBytes(StandardCharsets.UTF_8));
      strs.setSafe(3, "defg".getBytes(StandardCharsets.UTF_8));
      root.setRowCount(4);
      batches.add(unloader.getRecordBatch());

      root.allocateNew();
      ints.setSafe(0, 1);
      ints.setNull(1);
      ints.setSafe(2, 4);
      ints.setNull(3);
      strs.setSafe(0, "".getBytes(StandardCharsets.UTF_8));
      strs.setNull(1);
      strs.setSafe(2, "bc".getBytes(StandardCharsets.UTF_8));
      strs.setNull(3);
      root.setRowCount(4);
      batches.add(unloader.getRecordBatch());
      roundtrip(schema, batches);
    }
  }

  @Test
  public void roundtripDictionary() throws Exception {
    final ArrowType.Int indexType = new ArrowType.Int(32, true);
    final DictionaryEncoding encoding = new DictionaryEncoding(1L, false, indexType);
    final Schema schema = new Schema(Collections.singletonList(
        new Field("dict", new FieldType(/*nullable=*/true, indexType, encoding), Collections.emptyList())));
    final List<ArrowRecordBatch> batches = new ArrayList<>();
    try (final CDataDictionaryProvider provider = new CDataDictionaryProvider();
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final VarCharVector dictionary = new VarCharVector("values", allocator);
      dictionary.allocateNew();
      dictionary.setSafe(0, "foo".getBytes(StandardCharsets.UTF_8));
      dictionary.setSafe(1, "bar".getBytes(StandardCharsets.UTF_8));
      dictionary.setNull(2);
      dictionary.setValueCount(3);
      provider.put(new Dictionary(dictionary, encoding));
      final IntVector encoded = (IntVector) root.getVector(0);
      VectorUnloader unloader = new VectorUnloader(root);

      root.allocateNew();
      encoded.setSafe(0, 0);
      encoded.setSafe(1, 1);
      encoded.setSafe(2, 0);
      encoded.setSafe(3, 2);
      root.setRowCount(4);
      batches.add(unloader.getRecordBatch());

      root.allocateNew();
      encoded.setSafe(0, 0);
      encoded.setNull(1);
      encoded.setSafe(2, 1);
      encoded.setNull(3);
      root.setRowCount(4);
      batches.add(unloader.getRecordBatch());
      roundtrip(schema, batches, provider);
    }
  }

  @Test
  public void importReleasedStream() {
    try (final ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
      Exception e = assertThrows(IllegalStateException.class, () -> Data.importArrayStream(allocator, stream));
      assertThat(e).hasMessageContaining("Cannot import released ArrowArrayStream");
    }
  }

  @Test
  public void getNextError() throws Exception {
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("ints", new ArrowType.Int(32, true))));
    final List<ArrowRecordBatch> batches = new ArrayList<>();
    try (final ArrowReader source = new InMemoryArrowReader(allocator, schema, batches,
        new DictionaryProvider.MapDictionaryProvider()) {
      @Override
      public boolean loadNextBatch() throws IOException {
        throw new IOException("Failed to load batch!");
      }
    }; final ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, source, stream);
      try (final ArrowReader reader = Data.importArrayStream(allocator, stream)) {
        assertThat(reader.getVectorSchemaRoot().getSchema()).isEqualTo(schema);
        final IOException e = assertThrows(IOException.class, reader::loadNextBatch);
        assertThat(e).hasMessageContaining("Failed to load batch!");
        assertThat(e).hasMessageContaining("[errno ");
      }
    }
  }

  @Test
  public void getSchemaError() throws Exception {
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("ints", new ArrowType.Int(32, true))));
    final List<ArrowRecordBatch> batches = new ArrayList<>();
    try (final ArrowReader source = new InMemoryArrowReader(allocator, schema, batches,
        new DictionaryProvider.MapDictionaryProvider()) {
      @Override
      protected Schema readSchema() {
        throw new IllegalArgumentException("Failed to read schema!");
      }
    }; final ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, source, stream);
      try (final ArrowReader reader = Data.importArrayStream(allocator, stream)) {
        final IOException e = assertThrows(IOException.class, reader::getVectorSchemaRoot);
        assertThat(e).hasMessageContaining("Failed to read schema!");
        assertThat(e).hasMessageContaining("[errno ");
      }
    }
  }

  void roundtrip(Schema schema, List<ArrowRecordBatch> batches, DictionaryProvider provider) throws Exception {
    ArrowReader source = new InMemoryArrowReader(allocator, schema, batches, provider);

    try (final ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final VectorLoader loader = new VectorLoader(root);
      Data.exportArrayStream(allocator, source, stream);

      try (final ArrowReader reader = Data.importArrayStream(allocator, stream)) {
        assertThat(reader.getVectorSchemaRoot().getSchema()).isEqualTo(schema);

        for (ArrowRecordBatch batch : batches) {
          assertThat(reader.loadNextBatch()).isTrue();
          loader.load(batch);

          assertThat(reader.getVectorSchemaRoot().getRowCount()).isEqualTo(root.getRowCount());

          for (int i = 0; i < root.getFieldVectors().size(); i++) {
            final FieldVector expected = root.getVector(i);
            final FieldVector actual = reader.getVectorSchemaRoot().getVector(i);
            assertVectorsEqual(expected, actual);
          }
        }
        assertThat(reader.loadNextBatch()).isFalse();
        assertThat(reader.getDictionaryIds()).isEqualTo(provider.getDictionaryIds());
        for (Map.Entry<Long, Dictionary> entry : reader.getDictionaryVectors().entrySet()) {
          final FieldVector expected = provider.lookup(entry.getKey()).getVector();
          final FieldVector actual = entry.getValue().getVector();
          assertVectorsEqual(expected, actual);
        }
      }
    }
  }

  void roundtrip(Schema schema, List<ArrowRecordBatch> batches) throws Exception {
    roundtrip(schema, batches, new CDataDictionaryProvider());
  }

  private static void assertVectorsEqual(FieldVector expected, FieldVector actual) {
    assertThat(actual.getField().getType()).isEqualTo(expected.getField().getType());
    assertThat(actual.getValueCount()).isEqualTo(expected.getValueCount());
    final Range range = new Range(/*leftStart=*/0, /*rightStart=*/0, expected.getValueCount());
    assertThat(new RangeEqualsVisitor(expected, actual)
        .rangeEquals(range))
        .as("Vectors were not equal.\nExpected: %s\nGot: %s", expected, actual)
        .isTrue();
  }

  /**
   * An ArrowReader backed by a fixed list of batches.
   */
  static class InMemoryArrowReader extends ArrowReader {
    private final Schema schema;
    private final List<ArrowRecordBatch> batches;
    private final DictionaryProvider provider;
    private int nextBatch;

    InMemoryArrowReader(BufferAllocator allocator, Schema schema, List<ArrowRecordBatch> batches,
                        DictionaryProvider provider) {
      super(allocator);
      this.schema = schema;
      this.batches = batches;
      this.provider = provider;
      this.nextBatch = 0;
    }

    @Override
    public Dictionary lookup(long id) {
      return provider.lookup(id);
    }

    @Override
    public Set<Long> getDictionaryIds() {
      return provider.getDictionaryIds();
    }

    @Override
    public Map<Long, Dictionary> getDictionaryVectors() {
      return getDictionaryIds().stream().collect(Collectors.toMap(Function.identity(), this::lookup));
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      if (nextBatch < batches.size()) {
        VectorLoader loader = new VectorLoader(getVectorSchemaRoot());
        loader.load(batches.get(nextBatch++));
        return true;
      }
      return false;
    }

    @Override
    public long bytesRead() {
      return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
      try {
        AutoCloseables.close(batches);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    protected Schema readSchema() {
      return schema;
    }
  }
}

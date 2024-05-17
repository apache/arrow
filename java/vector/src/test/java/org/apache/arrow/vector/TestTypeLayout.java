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

package org.apache.arrow.vector;

import static org.apache.arrow.vector.testing.ValueVectorDataPopulator.setVector;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestTypeLayout {

  private static final Charset utf8Charset = StandardCharsets.UTF_8;
  private static final byte[] STR1 = "AAAAA1".getBytes(utf8Charset);
  private static final byte[] STR2 = "BBBBBBBBB2".getBytes(utf8Charset);
  private static final byte[] STR3 = "CCCC3".getBytes(utf8Charset);
  private static final byte[] STR4 = "DDDDDDDD4".getBytes(utf8Charset);
  private static final byte[] STR5 = "EEE5".getBytes(utf8Charset);
  private static final byte[] STR6 = "0123456789123456".getBytes(utf8Charset);

  private BufferAllocator allocator;

  @BeforeEach
  public void prepare() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @AfterEach
  public void shutdown() {
    allocator.close();
  }


  @Test
  public void testTypeBufferCount() {
    ArrowType type = new ArrowType.Int(8, true);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Union(UnionMode.Sparse, new int[2]);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Union(UnionMode.Dense, new int[1]);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Struct();
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.List();
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.FixedSizeList(5);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Map(false);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Decimal(10, 10, 128);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Decimal(10, 10, 256);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());


    type = new ArrowType.FixedSizeBinary(5);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Bool();
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Binary();
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Utf8();
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Null();
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Date(DateUnit.DAY);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Time(TimeUnit.MILLISECOND, 32);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Interval(IntervalUnit.DAY_TIME);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());

    type = new ArrowType.Duration(TimeUnit.MILLISECOND);
    assertEquals(TypeLayout.getTypeBufferCount(type, null),
        TypeLayout.getTypeLayout(type, null).getBufferLayouts().size());
  }

  private String generateRandomString(int length) {
    Random random = new Random();
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(random.nextInt(10)); // 0-9
    }
    return sb.toString();
  }

  @Test
  public void testTypeBufferCountInVectorsWithVariadicBuffers() {
    // empty vector
    try (ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      ArrowType type = viewVarCharVector.getMinorType().getType();
      assertEquals(TypeLayout.getTypeBufferCount(type, viewVarCharVector),
          TypeLayout.getTypeLayout(type, viewVarCharVector).getBufferLayouts().size());
    }
    // vector with long strings
    try (ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(32, 6);

      viewVarCharVector.setSafe(0, generateRandomString(8).getBytes());
      viewVarCharVector.setSafe(1, generateRandomString(12).getBytes());
      viewVarCharVector.setSafe(2, generateRandomString(14).getBytes());
      viewVarCharVector.setSafe(3, generateRandomString(18).getBytes());
      viewVarCharVector.setSafe(4, generateRandomString(22).getBytes());
      viewVarCharVector.setSafe(5, generateRandomString(24).getBytes());

      viewVarCharVector.setValueCount(6);

      ArrowType type = viewVarCharVector.getMinorType().getType();
      assertEquals(TypeLayout.getTypeBufferCount(type, viewVarCharVector),
          TypeLayout.getTypeLayout(type, viewVarCharVector).getBufferLayouts().size());
    }
  }

  @Test
  public void testVectorLoadUnload() {

    try (final ViewVarCharVector vector1 = new ViewVarCharVector("myvector", allocator)) {

      setVector(vector1, STR1, STR2, STR3, STR4, STR5, STR6);

      assertEquals(5, vector1.getLastSet());
      vector1.setValueCount(15);
      assertEquals(14, vector1.getLastSet());

      /* Check the vector output */
      assertArrayEquals(STR1, vector1.get(0));
      assertArrayEquals(STR2, vector1.get(1));
      assertArrayEquals(STR3, vector1.get(2));
      assertArrayEquals(STR4, vector1.get(3));
      assertArrayEquals(STR5, vector1.get(4));
      assertArrayEquals(STR6, vector1.get(5));

      Field field = vector1.getField();
      String fieldName = field.getName();

      List<Field> fields = new ArrayList<>();
      List<FieldVector> fieldVectors = new ArrayList<>();

      fields.add(field);
      fieldVectors.add(vector1);

      Schema schema = new Schema(fields);

      VectorSchemaRoot schemaRoot1 = new VectorSchemaRoot(schema, fieldVectors, vector1.getValueCount());
      VectorUnloader vectorUnloader = new VectorUnloader(schemaRoot1);

      try (
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
          BufferAllocator finalVectorsAllocator = allocator.newChildAllocator("new vector", 0, Long.MAX_VALUE);
          VectorSchemaRoot schemaRoot2 = VectorSchemaRoot.create(schema, finalVectorsAllocator);
      ) {

        VectorLoader vectorLoader = new VectorLoader(schemaRoot2);
        vectorLoader.load(recordBatch);

        ViewVarCharVector vector2 = (ViewVarCharVector) schemaRoot2.getVector(fieldName);
        /*
         * lastSet would have internally been set by VectorLoader.load() when it invokes
         * loadFieldBuffers.
         */
        assertEquals(14, vector2.getLastSet());
        vector2.setValueCount(25);
        assertEquals(24, vector2.getLastSet());

        /* Check the vector output */
        assertArrayEquals(STR1, vector2.get(0));
        assertArrayEquals(STR2, vector2.get(1));
        assertArrayEquals(STR3, vector2.get(2));
        assertArrayEquals(STR4, vector2.get(3));
        assertArrayEquals(STR5, vector2.get(4));
        assertArrayEquals(STR6, vector2.get(5));
      }
    }
  }

  @Test
  public void testVectorLoadUnload2() {

    try (final VarCharVector vector1 = new VarCharVector("myvector", allocator)) {

      setVector(vector1, STR1, STR2, STR3, STR4, STR5, STR6);

      assertEquals(5, vector1.getLastSet());
      vector1.setValueCount(15);
      assertEquals(14, vector1.getLastSet());

      /* Check the vector output */
      assertArrayEquals(STR1, vector1.get(0));
      assertArrayEquals(STR2, vector1.get(1));
      assertArrayEquals(STR3, vector1.get(2));
      assertArrayEquals(STR4, vector1.get(3));
      assertArrayEquals(STR5, vector1.get(4));
      assertArrayEquals(STR6, vector1.get(5));

      Field field = vector1.getField();
      String fieldName = field.getName();

      List<Field> fields = new ArrayList<>();
      List<FieldVector> fieldVectors = new ArrayList<>();

      fields.add(field);
      fieldVectors.add(vector1);

      Schema schema = new Schema(fields);

      VectorSchemaRoot schemaRoot1 = new VectorSchemaRoot(schema, fieldVectors, vector1.getValueCount());
      VectorUnloader vectorUnloader = new VectorUnloader(schemaRoot1);

      try (
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
          BufferAllocator finalVectorsAllocator = allocator.newChildAllocator("new vector", 0, Long.MAX_VALUE);
          VectorSchemaRoot schemaRoot2 = VectorSchemaRoot.create(schema, finalVectorsAllocator);
      ) {

        VectorLoader vectorLoader = new VectorLoader(schemaRoot2);
        vectorLoader.load(recordBatch);

        VarCharVector vector2 = (VarCharVector) schemaRoot2.getVector(fieldName);
        /*
         * lastSet would have internally been set by VectorLoader.load() when it invokes
         * loadFieldBuffers.
         */
        assertEquals(14, vector2.getLastSet());
        vector2.setValueCount(25);
        assertEquals(24, vector2.getLastSet());

        /* Check the vector output */
        assertArrayEquals(STR1, vector2.get(0));
        assertArrayEquals(STR2, vector2.get(1));
        assertArrayEquals(STR3, vector2.get(2));
        assertArrayEquals(STR4, vector2.get(3));
        assertArrayEquals(STR5, vector2.get(4));
        assertArrayEquals(STR6, vector2.get(5));
      }
    }
  }

  @Test
  public void testUnloadLoadAddPadding() throws IOException {
    int count = 10000;
    Schema schema;
    try (
        BufferAllocator originalVectorsAllocator =
            allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NonNullableStructVector parent = NonNullableStructVector.empty("parent", originalVectorsAllocator)) {

      // write some data
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();
      ListWriter list = rootWriter.list("list");
      IntWriter intWriter = list.integer();
      for (int i = 0; i < count; i++) {
        list.setPosition(i);
        list.startList();
        for (int j = 0; j < i % 4 + 1; j++) {
          intWriter.writeInt(i);
        }
        list.endList();
      }
      writer.setValueCount(count);

      // unload it
      FieldVector root = parent.getChild("root");
      schema = new Schema(root.getField().getChildren());
      VectorUnloader vectorUnloader = newVectorUnloader(root);
      try (
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
          BufferAllocator finalVectorsAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
          VectorSchemaRoot newRoot = VectorSchemaRoot.create(schema, finalVectorsAllocator);
      ) {
        List<ArrowBuf> oldBuffers = recordBatch.getBuffers();
        List<ArrowBuf> newBuffers = new ArrayList<>();
        for (ArrowBuf oldBuffer : oldBuffers) {
          long l = oldBuffer.readableBytes();
          if (l % 64 != 0) {
            // pad
            l = l + 64 - l % 64;
          }
          ArrowBuf newBuffer = allocator.buffer(l);
          for (long i = oldBuffer.readerIndex(); i < oldBuffer.writerIndex(); i++) {
            newBuffer.setByte(i - oldBuffer.readerIndex(), oldBuffer.getByte(i));
          }
          newBuffer.readerIndex(0);
          newBuffer.writerIndex(l);
          newBuffers.add(newBuffer);
        }

        try (ArrowRecordBatch newBatch =
            new ArrowRecordBatch(recordBatch.getLength(), recordBatch.getNodes(), newBuffers);) {
          // load it
          VectorLoader vectorLoader = new VectorLoader(newRoot);

          vectorLoader.load(newBatch);

          FieldReader reader = newRoot.getVector("list").getReader();
          for (int i = 0; i < count; i++) {
            reader.setPosition(i);
            List<Integer> expected = new ArrayList<>();
            for (int j = 0; j < i % 4 + 1; j++) {
              expected.add(i);
            }
            assertEquals(expected, reader.readObject());
          }
        }

        for (ArrowBuf newBuf : newBuffers) {
          newBuf.getReferenceManager().release();
        }
      }
    }
  }

  public static VectorUnloader newVectorUnloader(FieldVector root) {
    Schema schema = new Schema(root.getField().getChildren());
    int valueCount = root.getValueCount();
    List<FieldVector> fields = root.getChildrenFromFields();
    VectorSchemaRoot vsr = new VectorSchemaRoot(schema.getFields(), fields, valueCount);
    return new VectorUnloader(vsr);
  }
}

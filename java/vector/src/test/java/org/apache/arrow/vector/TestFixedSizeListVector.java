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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListReader;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFixedSizeListVector {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testIntType() {
    try (FixedSizeListVector vector = FixedSizeListVector.empty("list", /*size=*/2, allocator)) {
      IntVector nested = (IntVector) vector.addOrGetVector(FieldType.nullable(MinorType.INT.getType())).getVector();
      vector.allocateNew();

      for (int i = 0; i < 10; i++) {
        vector.setNotNull(i);
        nested.set(i * 2, i);
        nested.set(i * 2 + 1, i + 10);
      }
      vector.setValueCount(10);

      UnionFixedSizeListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        Assert.assertTrue(reader.isSet());
        Assert.assertTrue(reader.next());
        assertEquals(i, reader.reader().readInteger().intValue());
        Assert.assertTrue(reader.next());
        assertEquals(i + 10, reader.reader().readInteger().intValue());
        Assert.assertFalse(reader.next());
        assertEquals(Arrays.asList(i, i + 10), reader.readObject());
      }
    }
  }

  @Test
  public void testFloatTypeNullable() {
    try (FixedSizeListVector vector = FixedSizeListVector.empty("list", /*size=*/2, allocator)) {
      Float4Vector nested = (Float4Vector) vector.addOrGetVector(FieldType.nullable(MinorType.FLOAT4.getType()))
          .getVector();
      vector.allocateNew();

      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector.setNotNull(i);
          nested.set(i * 2, i + 0.1f);
          nested.set(i * 2 + 1, i + 10.1f);
        }
      }
      vector.setValueCount(10);

      UnionFixedSizeListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        if (i % 2 == 0) {
          Assert.assertTrue(reader.isSet());
          Assert.assertTrue(reader.next());
          assertEquals(i + 0.1f, reader.reader().readFloat(), 0.00001);
          Assert.assertTrue(reader.next());
          assertEquals(i + 10.1f, reader.reader().readFloat(), 0.00001);
          Assert.assertFalse(reader.next());
          assertEquals(Arrays.asList(i + 0.1f, i + 10.1f), reader.readObject());
        } else {
          Assert.assertFalse(reader.isSet());
          Assert.assertNull(reader.readObject());
        }
      }
    }
  }

  @Test
  public void testNestedInList() {
    try (ListVector vector = ListVector.empty("list", allocator)) {
      FixedSizeListVector tuples = (FixedSizeListVector) vector.addOrGetVector(
          FieldType.nullable(new ArrowType.FixedSizeList(2))).getVector();
      IntVector innerVector = (IntVector) tuples.addOrGetVector(FieldType.nullable(MinorType.INT.getType()))
          .getVector();
      vector.allocateNew();

      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          int position = vector.startNewValue(i);
          for (int j = 0; j < i % 7; j++) {
            tuples.setNotNull(position + j);
            innerVector.set((position + j) * 2, j);
            innerVector.set((position + j) * 2 + 1, j + 1);
          }
          vector.endValue(i, i % 7);
        }
      }
      vector.setValueCount(10);

      UnionListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        if (i % 2 == 0) {
          for (int j = 0; j < i % 7; j++) {
            Assert.assertTrue(reader.next());
            FieldReader innerListReader = reader.reader();
            for (int k = 0; k < 2; k++) {
              Assert.assertTrue(innerListReader.next());
              assertEquals(k + j, innerListReader.reader().readInteger().intValue());
            }
            Assert.assertFalse(innerListReader.next());
          }
          Assert.assertFalse(reader.next());
        } else {
          Assert.assertFalse(reader.isSet());
          Assert.assertNull(reader.readObject());
        }
      }
    }
  }

  @Test
  public void testTransferPair() {
    try (FixedSizeListVector from = new FixedSizeListVector(
        "from", allocator, new FieldType(true, new ArrowType.FixedSizeList(2), null), null);
         FixedSizeListVector to = new FixedSizeListVector(
             "to", allocator, new FieldType(true, new ArrowType.FixedSizeList(2), null), null)) {
      Float4Vector nested = (Float4Vector) from.addOrGetVector(FieldType.nullable(MinorType.FLOAT4.getType()))
          .getVector();
      from.allocateNew();

      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          from.setNotNull(i);
          nested.set(i * 2, i + 0.1f);
          nested.set(i * 2 + 1, i + 10.1f);
        }
      }
      from.setValueCount(10);

      TransferPair pair = from.makeTransferPair(to);

      pair.copyValueSafe(0, 1);
      pair.copyValueSafe(2, 2);
      to.copyFromSafe(4, 3, from);

      to.setValueCount(10);

      UnionFixedSizeListReader reader = to.getReader();

      reader.setPosition(0);
      Assert.assertFalse(reader.isSet());
      Assert.assertNull(reader.readObject());

      reader.setPosition(1);
      Assert.assertTrue(reader.isSet());
      Assert.assertTrue(reader.next());
      assertEquals(0.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertTrue(reader.next());
      assertEquals(10.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertFalse(reader.next());
      assertEquals(Arrays.asList(0.1f, 10.1f), reader.readObject());

      reader.setPosition(2);
      Assert.assertTrue(reader.isSet());
      Assert.assertTrue(reader.next());
      assertEquals(2.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertTrue(reader.next());
      assertEquals(12.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertFalse(reader.next());
      assertEquals(Arrays.asList(2.1f, 12.1f), reader.readObject());

      reader.setPosition(3);
      Assert.assertTrue(reader.isSet());
      Assert.assertTrue(reader.next());
      assertEquals(4.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertTrue(reader.next());
      assertEquals(14.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertFalse(reader.next());
      assertEquals(Arrays.asList(4.1f, 14.1f), reader.readObject());

      for (int i = 4; i < 10; i++) {
        reader.setPosition(i);
        Assert.assertFalse(reader.isSet());
        Assert.assertNull(reader.readObject());
      }
    }
  }

  @Test
  public void testConsistentChildName() throws Exception {
    try (FixedSizeListVector listVector = FixedSizeListVector.empty("sourceVector", /*size=*/2, allocator)) {
      String emptyListStr = listVector.getField().toString();
      Assert.assertTrue(emptyListStr.contains(ListVector.DATA_VECTOR_NAME));

      listVector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));
      String emptyVectorStr = listVector.getField().toString();
      Assert.assertTrue(emptyVectorStr.contains(ListVector.DATA_VECTOR_NAME));
    }
  }

  @Test
  public void testUnionFixedSizeListWriterWithNulls() throws Exception {
    /* Write to a decimal list vector
     * each list of size 3 and having its data values alternating between null and a non-null.
     * Read and verify
     */
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("vector", /*size=*/3, allocator)) {

      UnionFixedSizeListWriter writer = vector.getWriter();
      writer.allocate();

      final int valueCount = 100;

      for (int i = 0; i < valueCount; i++) {
        writer.startList();
        writer.decimal().writeDecimal(new BigDecimal(i));
        writer.writeNull();
        writer.decimal().writeDecimal(new BigDecimal(i * 3));
        writer.endList();
      }
      vector.setValueCount(valueCount);

      for (int i = 0; i < valueCount; i++) {
        List<BigDecimal> values = (List<BigDecimal>) vector.getObject(i);
        assertEquals(3, values.size());
        assertEquals(new BigDecimal(i), values.get(0));
        assertEquals(null, values.get(1));
        assertEquals(new BigDecimal(i * 3), values.get(2));
      }
    }
  }

  @Test
  public void testUnionFixedSizeListWriter() throws Exception {
    try (final FixedSizeListVector vector1 = FixedSizeListVector.empty("vector", /*size=*/3, allocator)) {

      UnionFixedSizeListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      int[] values1 = new int[] {1, 2, 3};
      int[] values2 = new int[] {4, 5, 6};
      int[] values3 = new int[] {7, 8, 9};

      //set some values
      writeListVector(vector1, writer1, values1);
      writeListVector(vector1, writer1, values2);
      writeListVector(vector1, writer1, values3);
      writer1.setValueCount(3);

      assertEquals(3, vector1.getValueCount());

      int[] realValue1 = convertListToIntArray(vector1.getObject(0));
      assertTrue(Arrays.equals(values1, realValue1));
      int[] realValue2 = convertListToIntArray(vector1.getObject(1));
      assertTrue(Arrays.equals(values2, realValue2));
      int[] realValue3 = convertListToIntArray(vector1.getObject(2));
      assertTrue(Arrays.equals(values3, realValue3));
    }
  }

  @Test
  public void testWriteDecimal() throws Exception {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("vector", /*size=*/3, allocator)) {

      UnionFixedSizeListWriter writer = vector.getWriter();
      writer.allocate();

      final int valueCount = 100;

      for (int i = 0; i < valueCount; i++) {
        writer.startList();
        writer.decimal().writeDecimal(new BigDecimal(i));
        writer.decimal().writeDecimal(new BigDecimal(i * 2));
        writer.decimal().writeDecimal(new BigDecimal(i * 3));
        writer.endList();
      }
      vector.setValueCount(valueCount);

      for (int i = 0; i < valueCount; i++) {
        List<BigDecimal> values = (List<BigDecimal>) vector.getObject(i);
        assertEquals(3, values.size());
        assertEquals(new BigDecimal(i), values.get(0));
        assertEquals(new BigDecimal(i * 2), values.get(1));
        assertEquals(new BigDecimal(i * 3), values.get(2));
      }
    }
  }

  @Test
  public void testDecimalIndexCheck() throws Exception {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("vector", /*size=*/3, allocator)) {

      UnionFixedSizeListWriter writer = vector.getWriter();
      writer.allocate();

      IllegalStateException e = assertThrows(IllegalStateException.class, () -> {
        writer.startList();
        writer.decimal().writeDecimal(new BigDecimal(1));
        writer.decimal().writeDecimal(new BigDecimal(2));
        writer.decimal().writeDecimal(new BigDecimal(3));
        writer.decimal().writeDecimal(new BigDecimal(4));
        writer.endList();
      });
      assertEquals("values at index 0 is greater than listSize 3", e.getMessage());
    }
  }


  @Test(expected = IllegalStateException.class)
  public void testWriteIllegalData() throws Exception {
    try (final FixedSizeListVector vector1 = FixedSizeListVector.empty("vector", /*size=*/3, allocator)) {

      UnionFixedSizeListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      int[] values1 = new int[] {1, 2, 3};
      int[] values2 = new int[] {4, 5, 6, 7, 8};

      //set some values
      writeListVector(vector1, writer1, values1);
      writeListVector(vector1, writer1, values2);
      writer1.setValueCount(3);

      assertEquals(3, vector1.getValueCount());
      int[] realValue1 = convertListToIntArray(vector1.getObject(0));
      assertTrue(Arrays.equals(values1, realValue1));
      int[] realValue2 = convertListToIntArray(vector1.getObject(1));
      assertTrue(Arrays.equals(values2, realValue2));
    }
  }

  @Test
  public void testSplitAndTransfer() throws Exception {
    try (final FixedSizeListVector vector1 = FixedSizeListVector.empty("vector", /*size=*/3, allocator)) {

      UnionFixedSizeListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      int[] values1 = new int[] {1, 2, 3};
      int[] values2 = new int[] {4, 5, 6};
      int[] values3 = new int[] {7, 8, 9};

      //set some values
      writeListVector(vector1, writer1, values1);
      writeListVector(vector1, writer1, values2);
      writeListVector(vector1, writer1, values3);
      writer1.setValueCount(3);

      TransferPair transferPair = vector1.getTransferPair(allocator);
      transferPair.splitAndTransfer(0, 2);
      FixedSizeListVector targetVector = (FixedSizeListVector) transferPair.getTo();

      assertEquals(2, targetVector.getValueCount());
      int[] realValue1 = convertListToIntArray(targetVector.getObject(0));
      assertArrayEquals(values1, realValue1);
      int[] realValue2 = convertListToIntArray(targetVector.getObject(1));
      assertArrayEquals(values2, realValue2);

      targetVector.clear();
    }
  }

  @Test
  public void testZeroWidthVector() {
    try (final FixedSizeListVector vector1 = FixedSizeListVector.empty("vector", /*size=*/0, allocator)) {

      UnionFixedSizeListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      int[] values1 = new int[] {};
      int[] values2 = new int[] {};
      int[] values3 = null;
      int[] values4 = new int[] {};

      //set some values
      writeListVector(vector1, writer1, values1);
      writeListVector(vector1, writer1, values2);
      writeListVector(vector1, writer1, values3);
      writeListVector(vector1, writer1, values4);
      writer1.setValueCount(4);

      assertEquals(4, vector1.getValueCount());

      int[] realValue1 = convertListToIntArray(vector1.getObject(0));
      assertArrayEquals(values1, realValue1);
      int[] realValue2 = convertListToIntArray(vector1.getObject(1));
      assertArrayEquals(values2, realValue2);
      assertNull(vector1.getObject(2));
      int[] realValue4 = convertListToIntArray(vector1.getObject(3));
      assertArrayEquals(values4, realValue4);
    }
  }

  @Test
  public void testVectorWithNulls() {
    try (final FixedSizeListVector vector1 = FixedSizeListVector.empty("vector", /*size=*/4, allocator)) {

      UnionFixedSizeListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      List<Integer> values1 = Arrays.asList(null, 1, 2, 3);
      List<Integer> values2 = Arrays.asList(4, null, 5, 6);
      List<Integer> values3 = null;
      List<Integer> values4 = Arrays.asList(7, 8, null, 9);

      //set some values
      writeListVector(vector1, writer1, values1);
      writeListVector(vector1, writer1, values2);
      writeListVector(vector1, writer1, values3);
      writeListVector(vector1, writer1, values4);
      writer1.setValueCount(4);

      assertEquals(4, vector1.getValueCount());

      List<?> realValue1 = vector1.getObject(0);
      assertEquals(values1, realValue1);
      List<?> realValue2 = vector1.getObject(1);
      assertEquals(values2, realValue2);
      List<?> realValue3 = vector1.getObject(2);
      assertEquals(values3, realValue3);
      List<?> realValue4 = vector1.getObject(3);
      assertEquals(values4, realValue4);
    }
  }

  @Test
  public void testWriteVarCharHelpers() throws Exception {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("vector", /*size=*/4, allocator)) {

      UnionFixedSizeListWriter writer = vector.getWriter();
      writer.allocate();

      writer.startList();
      writer.writeVarChar("row1,1");
      writer.writeVarChar(new Text("row1,2"));
      writer.writeNull();
      writer.writeNull();
      writer.endList();

      assertEquals("row1,1", vector.getObject(0).get(0).toString());
      assertEquals("row1,2", vector.getObject(0).get(1).toString());
    }
  }

  @Test
  public void testWriteLargeVarCharHelpers() throws Exception {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("vector", /*size=*/4, allocator)) {

      UnionFixedSizeListWriter writer = vector.getWriter();
      writer.allocate();

      writer.startList();
      writer.writeLargeVarChar("row1,1");
      writer.writeLargeVarChar(new Text("row1,2"));
      writer.writeNull();
      writer.writeNull();
      writer.endList();

      assertEquals("row1,1", vector.getObject(0).get(0).toString());
      assertEquals("row1,2", vector.getObject(0).get(1).toString());
    }
  }

  @Test
  public void testWriteVarBinaryHelpers() throws Exception {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("vector", /*size=*/4, allocator)) {

      UnionFixedSizeListWriter writer = vector.getWriter();
      writer.allocate();

      writer.startList();
      writer.writeVarBinary("row1,1".getBytes(StandardCharsets.UTF_8));
      writer.writeVarBinary("row1,2".getBytes(StandardCharsets.UTF_8), 0,
          "row1,2".getBytes(StandardCharsets.UTF_8).length);
      writer.writeVarBinary(ByteBuffer.wrap("row1,3".getBytes(StandardCharsets.UTF_8)));
      writer.writeVarBinary(ByteBuffer.wrap("row1,4".getBytes(StandardCharsets.UTF_8)), 0,
          "row1,4".getBytes(StandardCharsets.UTF_8).length);
      writer.endList();

      assertEquals("row1,1", new String((byte[]) vector.getObject(0).get(0), StandardCharsets.UTF_8));
      assertEquals("row1,2", new String((byte[]) vector.getObject(0).get(1), StandardCharsets.UTF_8));
      assertEquals("row1,3", new String((byte[]) vector.getObject(0).get(2), StandardCharsets.UTF_8));
      assertEquals("row1,4", new String((byte[]) vector.getObject(0).get(3), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testWriteLargeVarBinaryHelpers() throws Exception {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("vector", /*size=*/4, allocator)) {

      UnionFixedSizeListWriter writer = vector.getWriter();
      writer.allocate();

      writer.startList();
      writer.writeLargeVarBinary("row1,1".getBytes(StandardCharsets.UTF_8));
      writer.writeLargeVarBinary("row1,2".getBytes(StandardCharsets.UTF_8), 0,
          "row1,2".getBytes(StandardCharsets.UTF_8).length);
      writer.writeLargeVarBinary(ByteBuffer.wrap("row1,3".getBytes(StandardCharsets.UTF_8)));
      writer.writeLargeVarBinary(ByteBuffer.wrap("row1,4".getBytes(StandardCharsets.UTF_8)), 0,
          "row1,4".getBytes(StandardCharsets.UTF_8).length);
      writer.endList();

      assertEquals("row1,1", new String((byte[]) vector.getObject(0).get(0), StandardCharsets.UTF_8));
      assertEquals("row1,2", new String((byte[]) vector.getObject(0).get(1), StandardCharsets.UTF_8));
      assertEquals("row1,3", new String((byte[]) vector.getObject(0).get(2), StandardCharsets.UTF_8));
      assertEquals("row1,4", new String((byte[]) vector.getObject(0).get(3), StandardCharsets.UTF_8));
    }
  }

  private int[] convertListToIntArray(List<?> list) {
    int[] values = new int[list.size()];
    for (int i = 0; i < list.size(); i++) {
      values[i] = (int) list.get(i);
    }
    return values;
  }

  private void writeListVector(FixedSizeListVector vector, UnionFixedSizeListWriter writer, int[] values) {
    writer.startList();
    if (values != null) {
      for (int v : values) {
        writer.integer().writeInt(v);
      }
    } else {
      vector.setNull(writer.getPosition());
    }
    writer.endList();
  }

  private void writeListVector(FixedSizeListVector vector, UnionFixedSizeListWriter writer, List<Integer> values) {
    writer.startList();
    if (values != null) {
      for (Integer v : values) {
        if (v == null) {
          writer.writeNull();
        } else {
          writer.integer().writeInt(v);
        }
      }
    } else {
      vector.setNull(writer.getPosition());
    }
    writer.endList();
  }

}

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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSplitAndTransfer {
  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  private void populateVarcharVector(
      final VarCharVector vector, int valueCount, String[] compareArray) {
    for (int i = 0; i < valueCount; i += 3) {
      final String s = String.format("%010d", i);
      vector.set(i, s.getBytes(StandardCharsets.UTF_8));
      if (compareArray != null) {
        compareArray[i] = s;
      }
    }
    vector.setValueCount(valueCount);
  }

  private void populateBaseVariableWidthViewVector(
      final BaseVariableWidthViewVector vector, int valueCount, String[] compareArray) {
    for (int i = 0; i < valueCount; i += 3) {
      final String s = String.format("%010d", i);
      vector.set(i, s.getBytes(StandardCharsets.UTF_8));
      if (compareArray != null) {
        compareArray[i] = s;
      }
    }
    vector.setValueCount(valueCount);
  }

  private void populateIntVector(final IntVector vector, int valueCount) {
    for (int i = 0; i < valueCount; i++) {
      vector.set(i, i);
    }
    vector.setValueCount(valueCount);
  }

  private void populateDenseUnionVector(final DenseUnionVector vector, int valueCount) {
    VarCharVector varCharVector =
        vector.addOrGet("varchar", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
    BigIntVector intVector =
        vector.addOrGet("int", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);

    for (int i = 0; i < valueCount; i++) {
      vector.setTypeId(i, (byte) (i % 2));
      if (i % 2 == 0) {
        final String s = String.format("%010d", i);
        varCharVector.setSafe(i / 2, s.getBytes(StandardCharsets.UTF_8));
      } else {
        intVector.setSafe(i / 2, i);
      }
    }
    vector.setValueCount(valueCount);
  }

  @Test
  public void testWithEmptyVector() {
    // MapVector use TransferImpl from ListVector
    ListVector listVector = ListVector.empty("", allocator);
    TransferPair transferPair = listVector.getTransferPair(allocator);
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, transferPair.getTo().getValueCount());
    // BaseFixedWidthVector
    IntVector intVector = new IntVector("", allocator);
    transferPair = intVector.getTransferPair(allocator);
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, transferPair.getTo().getValueCount());
    // BaseVariableWidthVector
    VarCharVector varCharVector = new VarCharVector("", allocator);
    transferPair = varCharVector.getTransferPair(allocator);
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, transferPair.getTo().getValueCount());
    // BaseVariableWidthViewVector: ViewVarCharVector
    ViewVarCharVector viewVarCharVector = new ViewVarCharVector("", allocator);
    transferPair = viewVarCharVector.getTransferPair(allocator);
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, transferPair.getTo().getValueCount());
    // BaseVariableWidthVector: ViewVarBinaryVector
    ViewVarBinaryVector viewVarBinaryVector = new ViewVarBinaryVector("", allocator);
    transferPair = viewVarBinaryVector.getTransferPair(allocator);
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, transferPair.getTo().getValueCount());
    // BaseLargeVariableWidthVector
    LargeVarCharVector largeVarCharVector = new LargeVarCharVector("", allocator);
    transferPair = largeVarCharVector.getTransferPair(allocator);
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, transferPair.getTo().getValueCount());
    // StructVector
    StructVector structVector = StructVector.empty("", allocator);
    transferPair = structVector.getTransferPair(allocator);
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, transferPair.getTo().getValueCount());
    // FixedSizeListVector
    FixedSizeListVector fixedSizeListVector = FixedSizeListVector.empty("", 0, allocator);
    transferPair = fixedSizeListVector.getTransferPair(allocator);
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, transferPair.getTo().getValueCount());
    // FixedSizeBinaryVector
    FixedSizeBinaryVector fixedSizeBinaryVector = new FixedSizeBinaryVector("", allocator, 4);
    transferPair = fixedSizeBinaryVector.getTransferPair(allocator);
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, transferPair.getTo().getValueCount());
    // UnionVector
    UnionVector unionVector = UnionVector.empty("", allocator);
    transferPair = unionVector.getTransferPair(allocator);
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, transferPair.getTo().getValueCount());
    // DenseUnionVector
    DenseUnionVector duv = DenseUnionVector.empty("", allocator);
    transferPair = duv.getTransferPair(allocator);
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, transferPair.getTo().getValueCount());

    // non empty from vector

    // BaseFixedWidthVector
    IntVector fromIntVector = new IntVector("", allocator);
    fromIntVector.allocateNew(100);
    populateIntVector(fromIntVector, 100);
    transferPair = fromIntVector.getTransferPair(allocator);
    IntVector toIntVector = (IntVector) transferPair.getTo();
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, toIntVector.getValueCount());

    transferPair.splitAndTransfer(50, 0);
    assertEquals(0, toIntVector.getValueCount());

    transferPair.splitAndTransfer(100, 0);
    assertEquals(0, toIntVector.getValueCount());
    fromIntVector.clear();
    toIntVector.clear();

    // DenseUnionVector
    DenseUnionVector fromDuv = DenseUnionVector.empty("", allocator);
    populateDenseUnionVector(fromDuv, 100);
    transferPair = fromDuv.getTransferPair(allocator);
    DenseUnionVector toDUV = (DenseUnionVector) transferPair.getTo();
    transferPair.splitAndTransfer(0, 0);
    assertEquals(0, toDUV.getValueCount());

    transferPair.splitAndTransfer(50, 0);
    assertEquals(0, toDUV.getValueCount());

    transferPair.splitAndTransfer(100, 0);
    assertEquals(0, toDUV.getValueCount());
    fromDuv.clear();
    toDUV.clear();
  }

  @Test /* VarCharVector */
  public void test() throws Exception {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator)) {
      varCharVector.allocateNew(10000, 1000);

      final int valueCount = 500;
      final String[] compareArray = new String[valueCount];

      populateVarcharVector(varCharVector, valueCount, compareArray);

      final TransferPair tp = varCharVector.getTransferPair(allocator);
      final VarCharVector newVarCharVector = (VarCharVector) tp.getTo();
      final int[][] startLengths = {{0, 201}, {201, 0}, {201, 200}, {401, 99}};

      for (final int[] startLength : startLengths) {
        final int start = startLength[0];
        final int length = startLength[1];
        tp.splitAndTransfer(start, length);
        for (int i = 0; i < length; i++) {
          final boolean expectedSet = ((start + i) % 3) == 0;
          if (expectedSet) {
            final byte[] expectedValue = compareArray[start + i].getBytes(StandardCharsets.UTF_8);
            assertFalse(newVarCharVector.isNull(i));
            assertArrayEquals(expectedValue, newVarCharVector.get(i));
          } else {
            assertTrue(newVarCharVector.isNull(i));
          }
        }
        newVarCharVector.clear();
      }
    }
  }

  private void testView(BaseVariableWidthViewVector vector) {
    vector.allocateNew(10000, 1000);
    final int valueCount = 500;
    final String[] compareArray = new String[valueCount];

    populateBaseVariableWidthViewVector(vector, valueCount, compareArray);

    final TransferPair tp = vector.getTransferPair(allocator);
    final BaseVariableWidthViewVector newVector = (BaseVariableWidthViewVector) tp.getTo();
    ;
    final int[][] startLengths = {{0, 201}, {201, 0}, {201, 200}, {401, 99}};

    for (final int[] startLength : startLengths) {
      final int start = startLength[0];
      final int length = startLength[1];
      tp.splitAndTransfer(start, length);
      for (int i = 0; i < length; i++) {
        final boolean expectedSet = ((start + i) % 3) == 0;
        if (expectedSet) {
          final byte[] expectedValue = compareArray[start + i].getBytes(StandardCharsets.UTF_8);
          assertFalse(newVector.isNull(i));
          assertArrayEquals(expectedValue, newVector.get(i));
        } else {
          assertTrue(newVector.isNull(i));
        }
      }
      newVector.clear();
    }
  }

  @Test
  public void testUtf8View() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      testView(viewVarCharVector);
    }
  }

  @Test
  public void testBinaryView() throws Exception {
    try (final ViewVarBinaryVector viewVarBinaryVector =
        new ViewVarBinaryVector("myvector", allocator)) {
      testView(viewVarBinaryVector);
    }
  }

  @Test
  public void testMemoryConstrainedTransfer() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator)) {
      allocator.setLimit(32768); /* set limit of 32KB */

      varCharVector.allocateNew(10000, 1000);

      final int valueCount = 1000;

      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.getTransferPair(allocator);
      final VarCharVector newVarCharVector = (VarCharVector) tp.getTo();
      final int[][] startLengths = {{0, 700}, {700, 299}};

      for (final int[] startLength : startLengths) {
        final int start = startLength[0];
        final int length = startLength[1];
        tp.splitAndTransfer(start, length);
        newVarCharVector.clear();
      }
    }
  }

  private void testMemoryConstrainedTransferInViews(BaseVariableWidthViewVector vector) {
    // Here we have the target vector being transferred with a long string
    // hence, the data buffer will be allocated.
    // The default data buffer allocation takes
    // BaseVariableWidthViewVector.INITIAL_VIEW_VALUE_ALLOCATION *
    // BaseVariableWidthViewVector.ELEMENT_SIZE
    // set limit = BaseVariableWidthViewVector.INITIAL_VIEW_VALUE_ALLOCATION *
    // BaseVariableWidthViewVector.ELEMENT_SIZE
    final int setLimit =
        BaseVariableWidthViewVector.INITIAL_VIEW_VALUE_ALLOCATION
            * BaseVariableWidthViewVector.ELEMENT_SIZE;
    allocator.setLimit(setLimit);

    vector.allocateNew(16000, 1000);

    final int valueCount = 1000;

    populateBaseVariableWidthViewVector(vector, valueCount, null);

    final TransferPair tp = vector.getTransferPair(allocator);
    final BaseVariableWidthViewVector newVector = (BaseVariableWidthViewVector) tp.getTo();

    final int[][] startLengths = {{0, 700}, {700, 299}};

    for (final int[] startLength : startLengths) {
      final int start = startLength[0];
      final int length = startLength[1];
      tp.splitAndTransfer(start, length);
      newVector.clear();
    }
  }

  @Test
  public void testMemoryConstrainedTransferInUtf8Views() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      testMemoryConstrainedTransferInViews(viewVarCharVector);
    }
  }

  @Test
  public void testMemoryConstrainedTransferInBinaryViews() {
    try (final ViewVarBinaryVector viewVarBinaryVector =
        new ViewVarBinaryVector("myvector", allocator)) {
      testMemoryConstrainedTransferInViews(viewVarBinaryVector);
    }
  }

  @Test
  public void testTransfer() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator)) {
      varCharVector.allocateNew(10000, 1000);

      final int valueCount = 500;
      final String[] compareArray = new String[valueCount];
      populateVarcharVector(varCharVector, valueCount, compareArray);

      final TransferPair tp = varCharVector.getTransferPair(allocator);
      final VarCharVector newVarCharVector = (VarCharVector) tp.getTo();
      tp.transfer();

      assertEquals(0, varCharVector.valueCount);
      assertEquals(valueCount, newVarCharVector.valueCount);

      for (int i = 0; i < valueCount; i++) {
        final boolean expectedSet = (i % 3) == 0;
        if (expectedSet) {
          final byte[] expectedValue = compareArray[i].getBytes(StandardCharsets.UTF_8);
          assertFalse(newVarCharVector.isNull(i));
          assertArrayEquals(expectedValue, newVarCharVector.get(i));
        } else {
          assertTrue(newVarCharVector.isNull(i));
        }
      }

      newVarCharVector.clear();
    }
  }

  private void testTransferInViews(BaseVariableWidthViewVector vector) {
    vector.allocateNew(16000, 1000);

    final int valueCount = 500;
    final String[] compareArray = new String[valueCount];
    populateBaseVariableWidthViewVector(vector, valueCount, compareArray);

    final TransferPair tp = vector.getTransferPair(allocator);
    final BaseVariableWidthViewVector newVector = (BaseVariableWidthViewVector) tp.getTo();
    tp.transfer();

    assertEquals(0, vector.valueCount);
    assertEquals(valueCount, newVector.valueCount);

    for (int i = 0; i < valueCount; i++) {
      final boolean expectedSet = (i % 3) == 0;
      if (expectedSet) {
        final byte[] expectedValue = compareArray[i].getBytes(StandardCharsets.UTF_8);
        assertFalse(newVector.isNull(i));
        assertArrayEquals(expectedValue, newVector.get(i));
      } else {
        assertTrue(newVector.isNull(i));
      }
    }

    newVector.clear();
  }

  @Test
  public void testTransferInUtf8Views() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      testTransferInViews(viewVarCharVector);
    }
  }

  @Test
  public void testTransferInBinaryViews() {
    try (final ViewVarBinaryVector viewVarBinaryVector =
        new ViewVarBinaryVector("myvector", allocator)) {
      testTransferInViews(viewVarBinaryVector);
    }
  }

  @Test
  public void testCopyValueSafe() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator);
        final VarCharVector newVarCharVector = new VarCharVector("newvector", allocator)) {
      varCharVector.allocateNew(10000, 1000);

      final int valueCount = 500;
      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.makeTransferPair(newVarCharVector);

      // new vector memory is not pre-allocated, we expect copyValueSafe work fine.
      for (int i = 0; i < valueCount; i++) {
        tp.copyValueSafe(i, i);
      }
      newVarCharVector.setValueCount(valueCount);

      for (int i = 0; i < valueCount; i++) {
        final boolean expectedSet = (i % 3) == 0;
        if (expectedSet) {
          assertFalse(varCharVector.isNull(i));
          assertFalse(newVarCharVector.isNull(i));
          assertArrayEquals(varCharVector.get(i), newVarCharVector.get(i));
        } else {
          assertTrue(newVarCharVector.isNull(i));
        }
      }
    }
  }

  @Test
  public void testSplitAndTransferNon() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator)) {

      varCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.getTransferPair(allocator);
      VarCharVector newVarCharVector = (VarCharVector) tp.getTo();

      tp.splitAndTransfer(0, 0);
      assertEquals(0, newVarCharVector.getValueCount());

      newVarCharVector.clear();
    }
  }

  private void testSplitAndTransferNonInViews(BaseVariableWidthViewVector vector) {
    vector.allocateNew(16000, 1000);
    final int valueCount = 500;
    populateBaseVariableWidthViewVector(vector, valueCount, null);

    final TransferPair tp = vector.getTransferPair(allocator);
    BaseVariableWidthViewVector newVector = (BaseVariableWidthViewVector) tp.getTo();

    tp.splitAndTransfer(0, 0);
    assertEquals(0, newVector.getValueCount());

    newVector.clear();
  }

  @Test
  public void testSplitAndTransferNonInUtf8Views() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      testSplitAndTransferNonInViews(viewVarCharVector);
    }
  }

  @Test
  public void testSplitAndTransferNonInBinaryViews() {
    try (final ViewVarBinaryVector viewVarBinaryVector =
        new ViewVarBinaryVector("myvector", allocator)) {
      testSplitAndTransferNonInViews(viewVarBinaryVector);
    }
  }

  @Test
  public void testSplitAndTransferAll() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator)) {

      varCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.getTransferPair(allocator);
      VarCharVector newVarCharVector = (VarCharVector) tp.getTo();

      tp.splitAndTransfer(0, valueCount);
      assertEquals(valueCount, newVarCharVector.getValueCount());

      newVarCharVector.clear();
    }
  }

  private void testSplitAndTransferAllInViews(BaseVariableWidthViewVector vector) {
    vector.allocateNew(16000, 1000);
    final int valueCount = 500;
    populateBaseVariableWidthViewVector(vector, valueCount, null);

    final TransferPair tp = vector.getTransferPair(allocator);
    BaseVariableWidthViewVector newViewVarCharVector = (BaseVariableWidthViewVector) tp.getTo();

    tp.splitAndTransfer(0, valueCount);
    assertEquals(valueCount, newViewVarCharVector.getValueCount());

    newViewVarCharVector.clear();
  }

  @Test
  public void testSplitAndTransferAllInUtf8Views() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      testSplitAndTransferAllInViews(viewVarCharVector);
    }
  }

  @Test
  public void testSplitAndTransferAllInBinaryViews() {
    try (final ViewVarBinaryVector viewVarBinaryVector =
        new ViewVarBinaryVector("myvector", allocator)) {
      testSplitAndTransferAllInViews(viewVarBinaryVector);
    }
  }

  @Test
  public void testInvalidStartIndex() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator);
        final VarCharVector newVarCharVector = new VarCharVector("newvector", allocator)) {

      varCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.makeTransferPair(newVarCharVector);

      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> tp.splitAndTransfer(valueCount, 10));

      assertEquals(
          "Invalid parameters startIndex: 500, length: 10 for valueCount: 500", e.getMessage());

      newVarCharVector.clear();
    }
  }

  private void testInvalidStartIndexInViews(
      BaseVariableWidthViewVector vector, BaseVariableWidthViewVector newVector) {
    vector.allocateNew(16000, 1000);
    final int valueCount = 500;
    populateBaseVariableWidthViewVector(vector, valueCount, null);

    final TransferPair tp = vector.makeTransferPair(newVector);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> tp.splitAndTransfer(valueCount, 10));

    assertEquals(
        "Invalid parameters startIndex: 500, length: 10 for valueCount: 500", e.getMessage());

    newVector.clear();
  }

  @Test
  public void testInvalidStartIndexInUtf8Views() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator);
        final ViewVarCharVector newViewVarCharVector =
            new ViewVarCharVector("newvector", allocator)) {
      testInvalidStartIndexInViews(viewVarCharVector, newViewVarCharVector);
    }
  }

  @Test
  public void testInvalidStartIndexInBinaryViews() {
    try (final ViewVarBinaryVector viewVarBinaryVector =
            new ViewVarBinaryVector("myvector", allocator);
        final ViewVarBinaryVector newViewVarBinaryVector =
            new ViewVarBinaryVector("newvector", allocator)) {
      testInvalidStartIndexInViews(viewVarBinaryVector, newViewVarBinaryVector);
    }
  }

  @Test
  public void testInvalidLength() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator);
        final VarCharVector newVarCharVector = new VarCharVector("newvector", allocator)) {

      varCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.makeTransferPair(newVarCharVector);

      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class, () -> tp.splitAndTransfer(0, valueCount * 2));

      assertEquals(
          "Invalid parameters startIndex: 0, length: 1000 for valueCount: 500", e.getMessage());

      newVarCharVector.clear();
    }
  }

  private void testInvalidLengthInViews(
      BaseVariableWidthViewVector vector, BaseVariableWidthViewVector newVector) {
    vector.allocateNew(16000, 1000);
    final int valueCount = 500;
    populateBaseVariableWidthViewVector(vector, valueCount, null);

    final TransferPair tp = vector.makeTransferPair(newVector);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> tp.splitAndTransfer(0, valueCount * 2));

    assertEquals(
        "Invalid parameters startIndex: 0, length: 1000 for valueCount: 500", e.getMessage());

    newVector.clear();
  }

  @Test
  public void testInvalidLengthInUtf8Views() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator);
        final ViewVarCharVector newViewVarCharVector =
            new ViewVarCharVector("newvector", allocator)) {
      testInvalidLengthInViews(viewVarCharVector, newViewVarCharVector);
    }
  }

  @Test
  public void testInvalidLengthInBinaryViews() {
    try (final ViewVarBinaryVector viewVarBinaryVector =
            new ViewVarBinaryVector("myvector", allocator);
        final ViewVarBinaryVector newViewVarBinaryVector =
            new ViewVarBinaryVector("newvector", allocator)) {
      testInvalidLengthInViews(viewVarBinaryVector, newViewVarBinaryVector);
    }
  }

  @Test
  public void testZeroStartIndexAndLength() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator);
        final VarCharVector newVarCharVector = new VarCharVector("newvector", allocator)) {

      varCharVector.allocateNew(0, 0);
      final int valueCount = 0;
      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.makeTransferPair(newVarCharVector);

      tp.splitAndTransfer(0, 0);
      assertEquals(valueCount, newVarCharVector.getValueCount());

      newVarCharVector.clear();
    }
  }

  private void testZeroStartIndexAndLengthInViews(
      BaseVariableWidthViewVector vector, BaseVariableWidthViewVector newVector) {
    vector.allocateNew(0, 0);
    final int valueCount = 0;
    populateBaseVariableWidthViewVector(vector, valueCount, null);

    final TransferPair tp = vector.makeTransferPair(newVector);

    tp.splitAndTransfer(0, 0);
    assertEquals(valueCount, newVector.getValueCount());

    newVector.clear();
  }

  @Test
  public void testZeroStartIndexAndLengthInUtf8Views() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator);
        final ViewVarCharVector newViewVarCharVector =
            new ViewVarCharVector("newvector", allocator)) {
      testZeroStartIndexAndLengthInViews(viewVarCharVector, newViewVarCharVector);
    }
  }

  @Test
  public void testZeroStartIndexAndLengthInBinaryViews() {
    try (final ViewVarBinaryVector viewVarBinaryVector =
            new ViewVarBinaryVector("myvector", allocator);
        final ViewVarBinaryVector newViewVarBinaryVector =
            new ViewVarBinaryVector("newvector", allocator)) {
      testZeroStartIndexAndLengthInViews(viewVarBinaryVector, newViewVarBinaryVector);
    }
  }

  @Test
  public void testZeroLength() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator);
        final VarCharVector newVarCharVector = new VarCharVector("newvector", allocator)) {

      varCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.makeTransferPair(newVarCharVector);

      tp.splitAndTransfer(500, 0);
      assertEquals(0, newVarCharVector.getValueCount());

      newVarCharVector.clear();
    }
  }

  private void testZeroLengthInViews(
      BaseVariableWidthViewVector vector, BaseVariableWidthViewVector newVector) {
    vector.allocateNew(16000, 1000);
    final int valueCount = 500;
    populateBaseVariableWidthViewVector(vector, valueCount, null);

    final TransferPair tp = vector.makeTransferPair(newVector);

    tp.splitAndTransfer(500, 0);
    assertEquals(0, newVector.getValueCount());

    newVector.clear();
  }

  @Test
  public void testZeroLengthInUtf8Views() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator);
        final ViewVarCharVector newViewVarCharVector =
            new ViewVarCharVector("newvector", allocator)) {
      testZeroLengthInViews(viewVarCharVector, newViewVarCharVector);
    }
  }

  @Test
  public void testZeroLengthInBinaryViews() {
    try (final ViewVarBinaryVector viewVarBinaryVector =
            new ViewVarBinaryVector("myvector", allocator);
        final ViewVarBinaryVector newViewVarBinaryVector =
            new ViewVarBinaryVector("newvector", allocator)) {
      testZeroLengthInViews(viewVarBinaryVector, newViewVarBinaryVector);
    }
  }

  @Test
  public void testUnionVectorZeroStartIndexAndLength() {
    try (final UnionVector unionVector = UnionVector.empty("myvector", allocator);
        final UnionVector newUnionVector = UnionVector.empty("newvector", allocator)) {

      unionVector.allocateNew();
      final int valueCount = 0;
      unionVector.setValueCount(valueCount);

      final TransferPair tp = unionVector.makeTransferPair(newUnionVector);

      tp.splitAndTransfer(0, 0);
      assertEquals(valueCount, newUnionVector.getValueCount());

      newUnionVector.clear();
    }
  }

  @Test
  public void testFixedWidthVectorZeroStartIndexAndLength() {
    try (final IntVector intVector = new IntVector("myvector", allocator);
        final IntVector newIntVector = new IntVector("newvector", allocator)) {

      intVector.allocateNew(0);
      final int valueCount = 0;
      intVector.setValueCount(valueCount);

      final TransferPair tp = intVector.makeTransferPair(newIntVector);

      tp.splitAndTransfer(0, 0);
      assertEquals(valueCount, newIntVector.getValueCount());

      newIntVector.clear();
    }
  }

  @Test
  public void testBitVectorZeroStartIndexAndLength() {
    try (final BitVector bitVector = new BitVector("myvector", allocator);
        final BitVector newBitVector = new BitVector("newvector", allocator)) {

      bitVector.allocateNew(0);
      final int valueCount = 0;
      bitVector.setValueCount(valueCount);

      final TransferPair tp = bitVector.makeTransferPair(newBitVector);

      tp.splitAndTransfer(0, 0);
      assertEquals(valueCount, newBitVector.getValueCount());

      newBitVector.clear();
    }
  }

  @Test
  public void testFixedSizeListVectorZeroStartIndexAndLength() {
    try (final FixedSizeListVector listVector = FixedSizeListVector.empty("list", 4, allocator);
        final FixedSizeListVector newListVector =
            FixedSizeListVector.empty("newList", 4, allocator)) {

      listVector.allocateNew();
      final int valueCount = 0;
      listVector.setValueCount(valueCount);

      final TransferPair tp = listVector.makeTransferPair(newListVector);

      tp.splitAndTransfer(0, 0);
      assertEquals(valueCount, newListVector.getValueCount());

      newListVector.clear();
    }
  }

  @Test
  public void testListVectorZeroStartIndexAndLength() {
    try (final ListVector listVector = ListVector.empty("list", allocator);
        final ListVector newListVector = ListVector.empty("newList", allocator)) {

      listVector.allocateNew();
      final int valueCount = 0;
      listVector.setValueCount(valueCount);

      final TransferPair tp = listVector.makeTransferPair(newListVector);

      tp.splitAndTransfer(0, 0);
      assertEquals(valueCount, newListVector.getValueCount());

      newListVector.clear();
    }
  }

  @Test
  public void testStructVectorZeroStartIndexAndLength() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("k1", "v1");
    FieldType type = new FieldType(true, Struct.INSTANCE, null, metadata);
    try (final StructVector structVector = new StructVector("structvec", allocator, type, null);
        final StructVector newStructVector =
            new StructVector("newStructvec", allocator, type, null)) {

      structVector.allocateNew();
      final int valueCount = 0;
      structVector.setValueCount(valueCount);

      final TransferPair tp = structVector.makeTransferPair(newStructVector);

      tp.splitAndTransfer(0, 0);
      assertEquals(valueCount, newStructVector.getValueCount());

      newStructVector.clear();
    }
  }

  @Test
  public void testMapVectorZeroStartIndexAndLength() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("k1", "v1");
    FieldType type = new FieldType(true, new ArrowType.Map(false), null, metadata);
    try (final MapVector mapVector = new MapVector("mapVec", allocator, type, null);
        final MapVector newMapVector = new MapVector("newMapVec", allocator, type, null)) {

      mapVector.allocateNew();
      final int valueCount = 0;
      mapVector.setValueCount(valueCount);

      final TransferPair tp = mapVector.makeTransferPair(newMapVector);

      tp.splitAndTransfer(0, 0);
      assertEquals(valueCount, newMapVector.getValueCount());

      newMapVector.clear();
    }
  }
}

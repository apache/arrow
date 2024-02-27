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


import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseVariableWidthViewVector.InlineValueBuffer;
import org.apache.arrow.vector.BaseVariableWidthViewVector.ReferenceValueBuffer;
import org.apache.arrow.vector.BaseVariableWidthViewVector.ViewBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestVarCharViewVector {

  private static final byte[] STR0 = "0123456".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR1 = "012345678912".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR2 = "0123456789123".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR3 = "01234567891234567".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR4 = "01234567".getBytes(StandardCharsets.UTF_8);

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testTransfer() {
    try (BufferAllocator childAllocator1 = allocator.newChildAllocator("child1", 1000000, 1000000);
        ViewVarCharVector v1 = new ViewVarCharVector("v1", childAllocator1)) {
      v1.allocateNew();
      v1.setSafe(0, STR1);
      v1.setSafe(1, STR2);
      v1.setValueCount(2);

      System.out.println("v1 value count: " + v1.getValueCount());
      System.out.println(v1);
    }
  }

  @Test
  public void testAllocationLimits() {
    try (final LargeVarCharVector largeVarCharVector = new LargeVarCharVector("myvector", allocator)) {
      largeVarCharVector.allocateNew(17, 1);
      final int valueCount = 1;
      largeVarCharVector.set(0, STR3);
      largeVarCharVector.setValueCount(valueCount);
      System.out.println(largeVarCharVector);
      System.out.println(largeVarCharVector.getByteCapacity());
    }
  }

  @Test
  public void testInlineAllocation() {
    try (final ViewVarCharVector largeVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      largeVarCharVector.allocateNew(32, 3);
      final int valueCount = 3;
      largeVarCharVector.set(0, STR0);
      largeVarCharVector.set(1, STR1);
      largeVarCharVector.set(2, STR4);
      largeVarCharVector.setValueCount(valueCount);

      List<ViewBuffer> views = largeVarCharVector.views;
      List<ArrowBuf> dataBuffers = largeVarCharVector.dataBuffers;

      assert views.size() == 3;
      assert dataBuffers.isEmpty();

      ViewBuffer view0 = views.getFirst();
      assert view0 instanceof InlineValueBuffer;
      validateInlineValueBuffer(STR0, (InlineValueBuffer) view0);

      ViewBuffer view1 = views.get(1);
      assert view1 instanceof InlineValueBuffer;
      validateInlineValueBuffer(STR1, (InlineValueBuffer) view1);

      ViewBuffer view2 = views.get(1);
      assert view2 instanceof InlineValueBuffer;
      validateInlineValueBuffer(STR1, (InlineValueBuffer) view2);
    }
  }

  private void validateInlineValueBuffer(byte[] expected, InlineValueBuffer inlineValueBuffer) {
    assert inlineValueBuffer.getLength() == expected.length;
    byte[] viewBytes = new byte[expected.length];
    inlineValueBuffer.getValueBuffer().getBytes(0, viewBytes);
    String expectedStr = new String(viewBytes, StandardCharsets.UTF_8);
    String viewStr = new String(expected, StandardCharsets.UTF_8);
    assert expectedStr.equals(viewStr);
  }

  private void validateReferenceValueBuffer(byte[] expected, ReferenceValueBuffer referenceValueBuffer,
      List<ArrowBuf> dataBuffers, int startOffSet) {
    int bufId = referenceValueBuffer.getBufId();
    byte[] expectedPrefixBytes = new byte[4];
    System.arraycopy(expected, 0, expectedPrefixBytes, 0, 4);
    String expectedPrefix = new String(expectedPrefixBytes, StandardCharsets.UTF_8);
    String viewPrefix = new String(referenceValueBuffer.getPrefix(), StandardCharsets.UTF_8);
    assert expectedPrefix.equals(viewPrefix);
    ArrowBuf dataBuf = dataBuffers.get(bufId);
    byte[] dataBufBytes = new byte[expected.length];
    dataBuf.getBytes(startOffSet, dataBufBytes);
    String viewData = new String(dataBufBytes, StandardCharsets.UTF_8);
    String viewDataExpected = new String(expected, StandardCharsets.UTF_8);
    assert viewData.equals(viewDataExpected);
  }

  @Test
  public void testInlineAndReferenceAllocation() {
    try (final ViewVarCharVector largeVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      largeVarCharVector.allocateNew(32, 3);
      final int valueCount = 3;
      largeVarCharVector.set(0, STR1);
      largeVarCharVector.set(1, STR2);
      largeVarCharVector.set(2, STR3);
      largeVarCharVector.setValueCount(valueCount);

      List<ViewBuffer> views = largeVarCharVector.views;
      List<ArrowBuf> dataBuffers = largeVarCharVector.dataBuffers;

      assert views.size() == 3;
      assert dataBuffers.size() == 1;

      ViewBuffer view0 = views.getFirst();
      assert view0 instanceof InlineValueBuffer;
      InlineValueBuffer inlineValueBuffer = (InlineValueBuffer) view0;
      assert inlineValueBuffer.getLength() == STR1.length;
      byte[] view0Bytes = new byte[STR1.length];
      inlineValueBuffer.getValueBuffer().getBytes(0, view0Bytes);
      String expectedStr0 = new String(view0Bytes, StandardCharsets.UTF_8);
      String viewStr0 = new String(STR1, StandardCharsets.UTF_8);
      assert expectedStr0.equals(viewStr0);

      ViewBuffer view1 = views.get(1);
      assert view1 instanceof ReferenceValueBuffer;
      ReferenceValueBuffer referenceValueBuffer1 = (ReferenceValueBuffer) view1;
      int bufId0 = referenceValueBuffer1.getBufId();
      assert bufId0 == 0;
      byte[] expectedPrefix1Bytes = new byte[4];
      System.arraycopy(STR2, 0, expectedPrefix1Bytes, 0, 4);
      String expectedPrefix1 = new String(expectedPrefix1Bytes, StandardCharsets.UTF_8);
      String viewPrefix1 = new String(referenceValueBuffer1.getPrefix(), StandardCharsets.UTF_8);
      assert expectedPrefix1.equals(viewPrefix1);
      // second view
      ArrowBuf dataBuf1 = dataBuffers.get(bufId0);
      byte[] dataBuf1Bytes = new byte[STR2.length];
      dataBuf1.getBytes(0, dataBuf1Bytes);
      String viewData1 = new String(dataBuf1Bytes, StandardCharsets.UTF_8);
      String viewData1Expected = new String(STR2, StandardCharsets.UTF_8);
      assert viewData1.equals(viewData1Expected);
      // third view
      ViewBuffer view2 = views.get(2);
      assert view2 instanceof ReferenceValueBuffer;
      ReferenceValueBuffer referenceValueBuffer2 = (ReferenceValueBuffer) view2;
      int bufId2 = referenceValueBuffer2.getBufId();
      assert bufId2 == 0;
      byte[] expectedPrefix2Bytes = new byte[4];
      System.arraycopy(STR3, 0, expectedPrefix2Bytes, 0, 4);
      String expectedPrefix2 = new String(expectedPrefix2Bytes, StandardCharsets.UTF_8);
      String viewPrefix2 = new String(referenceValueBuffer1.getPrefix(), StandardCharsets.UTF_8);
      assert expectedPrefix2.equals(viewPrefix2);
      ArrowBuf dataBuf2 = dataBuffers.get(bufId2);
      byte[] dataBuf2Bytes = new byte[STR3.length];
      dataBuf2.getBytes(STR2.length, dataBuf2Bytes);
      String viewData2 = new String(dataBuf2Bytes, StandardCharsets.UTF_8);
      String viewData2Expected = new String(STR3, StandardCharsets.UTF_8);
      assert viewData2.equals(viewData2Expected);
      // all values
      byte[] dataBufAllBytes = new byte[STR2.length + STR3.length];
      dataBuf1.getBytes(0, dataBufAllBytes);
      String viewDataAll = new String(dataBufAllBytes, StandardCharsets.UTF_8);
      byte[] expectedAllBytes = new byte[STR2.length + STR3.length];
      System.arraycopy(STR2, 0, expectedAllBytes, 0, STR2.length);
      System.arraycopy(STR3, 0, expectedAllBytes, STR2.length, STR3.length);
      String expectedAll = new String(expectedAllBytes, StandardCharsets.UTF_8);
      assert viewDataAll.equals(expectedAll);
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testAllocationIndexOutOfBounds() {
    try (final ViewVarCharVector largeVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      largeVarCharVector.allocateNew(32, 3);
      final int valueCount = 3;
      largeVarCharVector.set(0, STR1);
      largeVarCharVector.set(1, STR2);
      largeVarCharVector.set(2, STR2);
      largeVarCharVector.setValueCount(valueCount);
      System.out.println(largeVarCharVector);
    }
  }

  @Test
  public void testBasicV2() {
    try (BufferAllocator childAllocator1 = allocator.newChildAllocator("child1", 1000000, 1000000);
        ViewVarCharVector v1 = new ViewVarCharVector("v1", childAllocator1);
        ViewVarCharVector v2 = new ViewVarCharVector("v2", childAllocator1)) {
      v1.allocateNew();
      v1.setSafe(0, STR1);
      v1.setValueCount(1);
      v2.allocateNew();
      v2.setSafe(0, STR2);
      v2.setValueCount(1);

      System.out.println("v1 value count: " + v1.getValueCount());
      System.out.println(v1);
      System.out.println("v2 value count: " + v2.getValueCount());
      System.out.println(v2);
    }
  }

  public static void setBytes(int index, byte[] bytes, LargeVarCharVector vector) {
    final long currentOffset =
        vector.offsetBuffer.getLong((long) index * BaseLargeVariableWidthVector.OFFSET_WIDTH);

    BitVectorHelper.setBit(vector.validityBuffer, index);
    vector.offsetBuffer.setLong(
        (long) (index + 1) * BaseLargeVariableWidthVector.OFFSET_WIDTH,
        currentOffset + bytes.length);
    vector.valueBuffer.setBytes(currentOffset, bytes, 0, bytes.length);
  }
}

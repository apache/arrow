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
  private static final byte[] STR5 = "012345678912345678".getBytes(StandardCharsets.UTF_8);

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
    // TODO: implement setSafe
    try (BufferAllocator childAllocator1 = allocator.newChildAllocator("child1", 1000000, 1000000);
        ViewVarCharVector v1 = new ViewVarCharVector("v1", childAllocator1)) {
      //      v1.allocateNew();
      //      v1.setSafe(0, STR1);
      //      v1.setSafe(1, STR2);
      //      v1.setValueCount(2);
      //
      //      System.out.println("v1 value count: " + v1.getValueCount());
      //      System.out.println(v1);
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

      ViewBuffer view0 = views.get(0);
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

  /**
  * Validate the InlineValueBuffer by comparing the expected byte array with the actual byte array stored in the buffer.
   * @param expected byte array expected to be compared
   * @param inlineValueBuffer InlineValueBuffer to be validated
  */
  private void validateInlineValueBuffer(byte[] expected, InlineValueBuffer inlineValueBuffer) {
    assert inlineValueBuffer.getLength() == expected.length;
    byte[] viewBytes = new byte[expected.length];
    inlineValueBuffer.getValueBuffer().getBytes(0, viewBytes);
    String expectedStr = new String(viewBytes, StandardCharsets.UTF_8);
    String viewStr = new String(expected, StandardCharsets.UTF_8);
    assert expectedStr.equals(viewStr);
  }

  /**
  * Validate the ReferenceValueBuffer by comparing the expected byte array with the actual byte array stored in the
   * buffer.
   * @param expected byte array expected to be compared
   * @param referenceValueBuffer ReferenceValueBuffer to be validated
   * @param dataBuffers List of ArrowBuf extracted from the ViewVarCharVector
   * @param startOffSet starting index to read from the ArrowBuf, useful when there are multiple values stored in the
   *                    same ArrowBuf
  */
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
  public void testReferenceAllocationInSameBuffer() {
    try (final ViewVarCharVector largeVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      largeVarCharVector.allocateNew(42, 3);
      final int valueCount = 3;
      largeVarCharVector.set(0, STR1);
      largeVarCharVector.set(1, STR2);
      largeVarCharVector.set(2, STR3);
      largeVarCharVector.setValueCount(valueCount);

      List<ViewBuffer> views = largeVarCharVector.views;
      List<ArrowBuf> dataBuffers = largeVarCharVector.dataBuffers;

      assert views.size() == 3;
      assert dataBuffers.size() == 1;

      ViewBuffer view0 = views.get(0);
      assert view0 instanceof InlineValueBuffer;
      validateInlineValueBuffer(STR1, (InlineValueBuffer) view0);

      ViewBuffer view1 = views.get(1);
      assert view1 instanceof ReferenceValueBuffer;
      validateReferenceValueBuffer(STR2, (ReferenceValueBuffer) view1, dataBuffers, 0);
      // third view
      ViewBuffer view2 = views.get(2);
      assert view2 instanceof ReferenceValueBuffer;
      validateReferenceValueBuffer(STR3, (ReferenceValueBuffer) view2, dataBuffers, STR2.length);
    }
  }

  @Test
  public void testReferenceAllocationInOtherBuffer() {
    try (final ViewVarCharVector largeVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      largeVarCharVector.allocateNew(18, 4);
      final int valueCount = 4;
      largeVarCharVector.set(0, STR1);
      largeVarCharVector.set(1, STR2);
      largeVarCharVector.set(2, STR3);
      largeVarCharVector.set(3, STR5);
      largeVarCharVector.setValueCount(valueCount);

      List<ViewBuffer> views = largeVarCharVector.views;
      List<ArrowBuf> dataBuffers = largeVarCharVector.dataBuffers;

      assert views.size() == 4;
      assert dataBuffers.size() == 2;

      ViewBuffer view0 = views.get(0);
      assert view0 instanceof InlineValueBuffer;
      validateInlineValueBuffer(STR1, (InlineValueBuffer) view0);

      ViewBuffer view1 = views.get(1);
      assert view1 instanceof ReferenceValueBuffer;
      validateReferenceValueBuffer(STR2, (ReferenceValueBuffer) view1, dataBuffers, 0);

      ViewBuffer view2 = views.get(2);
      assert view2 instanceof ReferenceValueBuffer;
      validateReferenceValueBuffer(STR3, (ReferenceValueBuffer) view2, dataBuffers, STR2.length);
      // third view
      ViewBuffer view3 = views.get(3);
      assert view3 instanceof ReferenceValueBuffer;
      validateReferenceValueBuffer(STR4, (ReferenceValueBuffer) view2, dataBuffers, 0);
      
      // checking if the first buffer in `dataBuffers` contains all the data as expected
      // view1 and view2 are in the same buffer since we choose 18 as the allocation size
      // nearest divisible by 8 and power of 2 allocation size is 32. 
      // So, the first buffer should contain STR2 with number of bytes 13.
      // the second buffer should contain STR3 with number of bytes 17.
      // Total space for view1 and view2 is 30 bytes, and we only have 2 bytes left. 
      // We have to allocate another buffer for view3. So total number of views is 3.
      // Total number of data buffers are 2 where the first buffer contains STR2 and STR3 
      // and the second buffer contains STR5.
      byte[] dataBufAllBytes = new byte[STR2.length + STR3.length];
      ArrowBuf dataBuf1 = dataBuffers.get(0);
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
    // TODO: implement setSafe
    try (BufferAllocator childAllocator1 = allocator.newChildAllocator("child1", 1000000, 1000000);
        ViewVarCharVector v1 = new ViewVarCharVector("v1", childAllocator1);
        ViewVarCharVector v2 = new ViewVarCharVector("v2", childAllocator1)) {
        //      v1.allocateNew();
        //      v1.setSafe(0, STR1);
        //      v1.setValueCount(1);
        //      v2.allocateNew();
        //      v2.setSafe(0, STR2);
        //      v2.setValueCount(1);
        //
        //      System.out.println("v1 value count: " + v1.getValueCount());
        //      System.out.println(v1);
        //      System.out.println("v2 value count: " + v2.getValueCount());
        //      System.out.println(v2);
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

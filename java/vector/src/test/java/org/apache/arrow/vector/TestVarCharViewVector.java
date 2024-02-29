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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
  private static final byte[] STR6 = "01234567891234567890".getBytes(StandardCharsets.UTF_8);

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
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(48, 3);
      final int valueCount = 3;
      viewVarCharVector.set(0, STR0);
      viewVarCharVector.set(1, STR1);
      viewVarCharVector.set(2, STR4);
      viewVarCharVector.setValueCount(valueCount);

      byte[] view1 = viewVarCharVector.get(0);
      byte[] view2 = viewVarCharVector.get(1);
      byte[] view3 = viewVarCharVector.get(2);

      assert view1 != null;
      assert view2 != null;
      assert view3 != null;

      System.out.println(new String(view1, StandardCharsets.UTF_8));
      System.out.println(new String(view2, StandardCharsets.UTF_8));
      System.out.println(new String(view3, StandardCharsets.UTF_8));

      // TODO: assert length, offset, and values per each view
      // refer to testSetLastSetUsage
    }
  }

  @Test
  public void testReferenceAllocationInSameBuffer() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(48, 4);
      final int valueCount = 4;
      viewVarCharVector.set(0, STR1);
      viewVarCharVector.set(1, STR2);
      viewVarCharVector.set(2, STR3);
      viewVarCharVector.set(3, generateRandomString(34).getBytes(StandardCharsets.UTF_8));
      viewVarCharVector.setValueCount(valueCount);

      byte[] view1 = viewVarCharVector.get(0);
      byte[] view2 = viewVarCharVector.get(1);
      byte[] view3 = viewVarCharVector.get(2);
      byte[] view4 = viewVarCharVector.get(3);

      assert view1 != null;
      assert view2 != null;
      assert view3 != null;
      assert view4 != null;

      System.out.println(new String(view1, StandardCharsets.UTF_8));
      System.out.println(new String(view2, StandardCharsets.UTF_8));
      System.out.println(new String(view3, StandardCharsets.UTF_8));
      System.out.println(new String(view4, StandardCharsets.UTF_8));

      System.out.println(viewVarCharVector.dataBuffers.size()); // 1
    }
  }

  @Test
  public void testReferenceAllocationInOtherBuffer() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(48, 4);
      final int valueCount = 4;
      viewVarCharVector.set(0, STR1);
      viewVarCharVector.set(1, STR2);
      viewVarCharVector.set(2, STR3);
      viewVarCharVector.set(3, generateRandomString(35).getBytes(StandardCharsets.UTF_8));
      viewVarCharVector.setValueCount(valueCount);

      byte[] view1 = viewVarCharVector.get(0);
      byte[] view2 = viewVarCharVector.get(1);
      byte[] view3 = viewVarCharVector.get(2);
      byte[] view4 = viewVarCharVector.get(3);

      assert view1 != null;
      assert view2 != null;
      assert view3 != null;
      assert view4 != null;

      System.out.println(new String(view1, StandardCharsets.UTF_8));
      System.out.println(new String(view2, StandardCharsets.UTF_8));
      System.out.println(new String(view3, StandardCharsets.UTF_8));
      System.out.println(new String(view4, StandardCharsets.UTF_8));

      System.out.println(viewVarCharVector.dataBuffers.size()); // 2
    }
  }

  @Test
  public void testMixedAllocation() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(128, 6);
      final int valueCount = 6;
      viewVarCharVector.set(0, STR1);
      viewVarCharVector.set(1, STR2);
      viewVarCharVector.set(2, STR3);
      viewVarCharVector.set(3, generateRandomString(35).getBytes(StandardCharsets.UTF_8));
      viewVarCharVector.set(4, STR1);
      viewVarCharVector.set(5, generateRandomString(40).getBytes(StandardCharsets.UTF_8));
      viewVarCharVector.setValueCount(valueCount);

      byte[] view1 = viewVarCharVector.get(0);
      byte[] view2 = viewVarCharVector.get(1);
      byte[] view3 = viewVarCharVector.get(2);
      byte[] view4 = viewVarCharVector.get(3);

      assert view1 != null;
      assert view2 != null;
      assert view3 != null;
      assert view4 != null;

      System.out.println(new String(view1, StandardCharsets.UTF_8));
      System.out.println(new String(view2, StandardCharsets.UTF_8));
      System.out.println(new String(view3, StandardCharsets.UTF_8));
      System.out.println(new String(view4, StandardCharsets.UTF_8));

      System.out.println(viewVarCharVector.dataBuffers.size()); // 2
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

  @Test
  public void testBufferCreation() {
    // inline buffer creation
    final int VIEW_BUFFER_SIZE = 16;
    final int LENGTH_WIDTH = 4;
    final int INLINE_BUF_DATA_LENGTH = 12;
    final int PREFIX_WIDTH = 4;
    final int BUF_INDEX_WIDTH = 4;
    final int BUF_OFFSET_WIDTH = 4;
    int lastWriteIndexInLineBuffer = 0;
    List<ArrowBuf> dataBuffer = new ArrayList<>();
    ArrowBuf inlineBuf = allocator.buffer(BaseVariableWidthViewVector.INITIAL_VALUE_ALLOCATION * 8);
    inlineBuf.setInt(0, STR0.length);

    inlineBuf.readerIndex(0);
    inlineBuf.writerIndex(LENGTH_WIDTH);

    inlineBuf.setBytes(LENGTH_WIDTH, STR0, 0, STR0.length);
    inlineBuf.writerIndex(VIEW_BUFFER_SIZE);


    // reference buffer creation
    // set buffer id
    if (dataBuffer.isEmpty()) {
      // add first buffer
      ArrowBuf dataBuf = allocator.buffer(BaseVariableWidthViewVector.INITIAL_VALUE_ALLOCATION * 8);
      // set length
      inlineBuf.setInt(inlineBuf.writerIndex(), STR2.length);
      inlineBuf.writerIndex(inlineBuf.writerIndex() + LENGTH_WIDTH);
      // set prefix
      inlineBuf.setBytes(inlineBuf.writerIndex(), STR2, 0, PREFIX_WIDTH);
      inlineBuf.writerIndex(inlineBuf.writerIndex() + PREFIX_WIDTH);
      // set buf id
      inlineBuf.setInt(inlineBuf.writerIndex(), 0);
      inlineBuf.writerIndex(inlineBuf.writerIndex() + BUF_INDEX_WIDTH);
      // add offset
      inlineBuf.setInt(inlineBuf.writerIndex(), 0);
      inlineBuf.writerIndex(inlineBuf.writerIndex() + BUF_OFFSET_WIDTH);
      dataBuf.setBytes(0, STR2, 0, STR2.length);
      dataBuf.readerIndex(0);
      dataBuf.writerIndex(STR2.length);
      dataBuffer.add(dataBuf);
    } else {
      // continue from last buffer
    }

    System.out.println("Reading Inline Buffer");
    System.out.println("Length: " + inlineBuf.getInt(0));
    byte[] bytes = new byte[STR0.length];
    inlineBuf.getBytes(LENGTH_WIDTH, bytes, 0, bytes.length);
    String str = new String(bytes, StandardCharsets.UTF_8);
    System.out.println("Value: " + str);

    System.out.println("Read Next Buffer");
    inlineBuf.readerIndex(inlineBuf.readerIndex() + VIEW_BUFFER_SIZE);
    int refBufLength = inlineBuf.getInt(inlineBuf.readerIndex());
    System.out.println("Length: " + refBufLength);
    if (refBufLength > 12) {
      // must be a reference buffer
      // read prefix
      byte[] prefixBytes = new byte[PREFIX_WIDTH];
      inlineBuf.readerIndex(inlineBuf.readerIndex() + PREFIX_WIDTH);
      inlineBuf.getBytes(inlineBuf.readerIndex(), prefixBytes, 0, PREFIX_WIDTH);
      String prefix = new String(prefixBytes, StandardCharsets.UTF_8);
      System.out.println("PREFIX : " + prefix);
      // read buf Id
      inlineBuf.readerIndex(inlineBuf.readerIndex() + BUF_INDEX_WIDTH);
      int bufId = inlineBuf.getInt(inlineBuf.readerIndex());
      System.out.println("Buf Id: " + bufId);
      // read offset
      inlineBuf.readerIndex(inlineBuf.readerIndex() + BUF_OFFSET_WIDTH);
      int offset = inlineBuf.getInt(inlineBuf.readerIndex());
      System.out.println("Offset: " + offset);
      ArrowBuf refBuf = dataBuffer.get(bufId);
      byte[] refBytes = new byte[refBufLength];
      refBuf.getBytes(offset, refBytes, 0, refBytes.length);
      String refStr = new String(refBytes, StandardCharsets.UTF_8);
      System.out.println("Value: " + refStr);
    } else {
      // inline buffer
    }

    inlineBuf.clear();
    inlineBuf.close();
    ArrowBuf referenceBuf = dataBuffer.get(0);
    referenceBuf.clear();
    referenceBuf.close();
    allocator.close();
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

  private String generateRandomString(int length) {
    Random random = new Random();
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(random.nextInt(10)); // 0-9
    }
    return sb.toString();
  }
}

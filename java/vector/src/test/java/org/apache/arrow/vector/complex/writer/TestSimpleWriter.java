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

package org.apache.arrow.vector.complex.writer;

import java.nio.ByteBuffer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.impl.LargeVarBinaryWriterImpl;
import org.apache.arrow.vector.complex.impl.VarBinaryWriterImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSimpleWriter {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testWriteByteArrayToVarBinary() {
    try (VarBinaryVector vector = new VarBinaryVector("test", allocator);
         VarBinaryWriterImpl writer = new VarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      writer.writeToVarBinary(input);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(input, result);
    }
  }

  @Test
  public void testWriteByteArrayWithOffsetToVarBinary() {
    try (VarBinaryVector vector = new VarBinaryVector("test", allocator);
         VarBinaryWriterImpl writer = new VarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      writer.writeToVarBinary(input, 1, 1);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(new byte[] { 0x02 }, result);
    }
  }

  @Test
  public void testWriteByteBufferToVarBinary() {
    try (VarBinaryVector vector = new VarBinaryVector("test", allocator);
         VarBinaryWriterImpl writer = new VarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      ByteBuffer buffer = ByteBuffer.wrap(input);
      writer.writeToVarBinary(buffer);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(input, result);
    }
  }

  @Test
  public void testWriteByteBufferWithOffsetToVarBinary() {
    try (VarBinaryVector vector = new VarBinaryVector("test", allocator);
         VarBinaryWriterImpl writer = new VarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      ByteBuffer buffer = ByteBuffer.wrap(input);
      writer.writeToVarBinary(buffer, 1, 1);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(new byte[] { 0x02 }, result);
    }
  }

  @Test
  public void testWriteByteArrayToLargeVarBinary() {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("test", allocator);
         LargeVarBinaryWriterImpl writer = new LargeVarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      writer.writeToLargeVarBinary(input);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(input, result);
    }
  }

  @Test
  public void testWriteByteArrayWithOffsetToLargeVarBinary() {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("test", allocator);
         LargeVarBinaryWriterImpl writer = new LargeVarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      writer.writeToLargeVarBinary(input, 1, 1);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(new byte[] { 0x02 }, result);
    }
  }

  @Test
  public void testWriteByteBufferToLargeVarBinary() {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("test", allocator);
         LargeVarBinaryWriterImpl writer = new LargeVarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      ByteBuffer buffer = ByteBuffer.wrap(input);
      writer.writeToLargeVarBinary(buffer);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(input, result);
    }
  }

  @Test
  public void testWriteByteBufferWithOffsetToLargeVarBinary() {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("test", allocator);
         LargeVarBinaryWriterImpl writer = new LargeVarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      ByteBuffer buffer = ByteBuffer.wrap(input);
      writer.writeToLargeVarBinary(buffer, 1, 1);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(new byte[] { 0x02 }, result);
    }
  }
}

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
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.impl.LargeVarBinaryWriterImpl;
import org.apache.arrow.vector.complex.impl.LargeVarCharWriterImpl;
import org.apache.arrow.vector.complex.impl.VarBinaryWriterImpl;
import org.apache.arrow.vector.complex.impl.VarCharWriterImpl;
import org.apache.arrow.vector.util.Text;
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
  public void testWriteByteArrayToVarBinary() throws Exception {
    try (VarBinaryVector vector = new VarBinaryVector("test", allocator);
         VarBinaryWriter writer = new VarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      writer.writeVarBinary(input);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(input, result);
    }
  }

  @Test
  public void testWriteByteArrayWithOffsetToVarBinary() throws Exception {
    try (VarBinaryVector vector = new VarBinaryVector("test", allocator);
         VarBinaryWriter writer = new VarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      writer.writeVarBinary(input, 1, 1);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(new byte[] { 0x02 }, result);
    }
  }

  @Test
  public void testWriteByteBufferToVarBinary() throws Exception {
    try (VarBinaryVector vector = new VarBinaryVector("test", allocator);
         VarBinaryWriter writer = new VarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      ByteBuffer buffer = ByteBuffer.wrap(input);
      writer.writeVarBinary(buffer);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(input, result);
    }
  }

  @Test
  public void testWriteByteBufferWithOffsetToVarBinary() throws Exception {
    try (VarBinaryVector vector = new VarBinaryVector("test", allocator);
         VarBinaryWriter writer = new VarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      ByteBuffer buffer = ByteBuffer.wrap(input);
      writer.writeVarBinary(buffer, 1, 1);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(new byte[] { 0x02 }, result);
    }
  }

  @Test
  public void testWriteByteArrayToLargeVarBinary() throws Exception {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("test", allocator);
         LargeVarBinaryWriter writer = new LargeVarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      writer.writeLargeVarBinary(input);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(input, result);
    }
  }

  @Test
  public void testWriteByteArrayWithOffsetToLargeVarBinary() throws Exception {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("test", allocator);
         LargeVarBinaryWriter writer = new LargeVarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      writer.writeLargeVarBinary(input, 1, 1);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(new byte[] { 0x02 }, result);
    }
  }

  @Test
  public void testWriteByteBufferToLargeVarBinary() throws Exception {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("test", allocator);
         LargeVarBinaryWriter writer = new LargeVarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      ByteBuffer buffer = ByteBuffer.wrap(input);
      writer.writeLargeVarBinary(buffer);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(input, result);
    }
  }

  @Test
  public void testWriteByteBufferWithOffsetToLargeVarBinary() throws Exception {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("test", allocator);
         LargeVarBinaryWriter writer = new LargeVarBinaryWriterImpl(vector)) {
      byte[] input = new byte[] { 0x01, 0x02 };
      ByteBuffer buffer = ByteBuffer.wrap(input);
      writer.writeLargeVarBinary(buffer, 1, 1);
      byte[] result = vector.get(0);
      Assert.assertArrayEquals(new byte[] { 0x02 }, result);
    }
  }

  @Test
  public void testWriteStringToVarChar() throws Exception {
    try (VarCharVector vector = new VarCharVector("test", allocator);
         VarCharWriter writer = new VarCharWriterImpl(vector)) {
      String input = "testInput";
      writer.writeVarChar(input);
      String result = vector.getObject(0).toString();
      Assert.assertEquals(input, result);
    }
  }

  @Test
  public void testWriteTextToVarChar() throws Exception {
    try (VarCharVector vector = new VarCharVector("test", allocator);
         VarCharWriter writer = new VarCharWriterImpl(vector)) {
      String input = "testInput";
      writer.writeVarChar(new Text(input));
      String result = vector.getObject(0).toString();
      Assert.assertEquals(input, result);
    }
  }

  @Test
  public void testWriteStringToLargeVarChar() throws Exception {
    try (LargeVarCharVector vector = new LargeVarCharVector("test", allocator);
         LargeVarCharWriter writer = new LargeVarCharWriterImpl(vector)) {
      String input = "testInput";
      writer.writeLargeVarChar(input);
      String result = vector.getObject(0).toString();
      Assert.assertEquals(input, result);
    }
  }

  @Test
  public void testWriteTextToLargeVarChar() throws Exception {
    try (LargeVarCharVector vector = new LargeVarCharVector("test", allocator);
         LargeVarCharWriter writer = new LargeVarCharWriterImpl(vector)) {
      String input = "testInput";
      writer.writeLargeVarChar(new Text(input));
      String result = vector.getObject(0).toString();
      Assert.assertEquals(input, result);
    }
  }
}

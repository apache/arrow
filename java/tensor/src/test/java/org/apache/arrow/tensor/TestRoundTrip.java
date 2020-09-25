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

package org.apache.arrow.tensor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.Tensor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRoundTrip {
  private static final double EPSILON = 1e-6;
  private BufferAllocator allocator;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testMetadata() {
    long[] shape = new long[]{2, 3};
    long[] rowStrides = BaseTensor.rowMajorStrides(Float8Tensor.TYPE_WIDTH, shape);
    double[] rowMajorSource = new double[] {1, 2, 3, 4, 5, 6};
    try (Float8Tensor tensor = new Float8Tensor(allocator, shape, null, rowStrides)) {
      tensor.copyFrom(rowMajorSource);
      final Message msg = Message.getRootAsMessage(tensor.getAsMessage(new IpcOption()));
      assertEquals(MessageHeader.Tensor, msg.headerType());
      final Tensor tensorMeta = (Tensor) msg.header(new Tensor());
      assertNotNull(tensorMeta);
      assertEquals(tensor.getShape().length, tensorMeta.shapeLength());
      assertEquals(tensor.getStrides().length, tensorMeta.stridesLength());
      assertTrue(msg.bodyLength() >= BaseTensor.minBufferSize(tensor.getShape(), tensor.getStrides()));
    }
  }

  @Test
  public void testRoundTrip() throws IOException {
    long[] shape = new long[]{2, 3};
    long[] rowStrides = BaseTensor.rowMajorStrides(Float8Tensor.TYPE_WIDTH, shape);
    double[] rowMajorSource = new double[] {1, 2, 3, 4, 5, 6};

    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (Float8Tensor tensor = new Float8Tensor(allocator, shape, null, rowStrides)) {
      tensor.copyFrom(rowMajorSource);
      try (final WriteChannel ch = new WriteChannel(Channels.newChannel(buf))) {
        tensor.write(ch, new IpcOption());
      }
    }
    try (final ReadChannel ch = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(buf.toByteArray())))) {
      try (final Float8Tensor tensor = (Float8Tensor) BaseTensor.read(ch, allocator)) {
        double[] target = new double[rowMajorSource.length];
        tensor.copyTo(target);
        assertArrayEquals(rowMajorSource, target, EPSILON);
      }
    }
  }
}

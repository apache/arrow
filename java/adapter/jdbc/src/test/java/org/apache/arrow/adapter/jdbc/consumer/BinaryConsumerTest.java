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

package org.apache.arrow.adapter.jdbc.consumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.junit.Test;

public class BinaryConsumerTest extends AbstractConsumerTest {

  private static final int INITIAL_VALUE_ALLOCATION = BaseValueVector.INITIAL_VALUE_ALLOCATION;
  private static final int DEFAULT_RECORD_BYTE_COUNT = 8;

  interface InputStreamConsumer {
    void consume(BinaryConsumer consumer) throws IOException;
  }

  protected void assertConsume(boolean nullable, InputStreamConsumer dataConsumer, byte[][] expect) throws IOException {
    try (final VarBinaryVector vector = new VarBinaryVector("binary", allocator)) {
      BinaryConsumer consumer = BinaryConsumer.createConsumer(vector, 0, nullable);
      dataConsumer.consume(consumer);
      assertEquals(expect.length - 1, vector.getLastSet());
      for (int i = 0; i < expect.length; i++) {
        byte[] value = expect[i];
        if (value == null) {
          assertTrue(vector.isNull(i));
        } else {
          assertArrayEquals(expect[i], vector.get(i));
        }
      }
    }
  }

  private byte[] createBytes(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (i % 1024);
    }
    return bytes;
  }


  public void testConsumeInputStream(byte[][] values, boolean nullable) throws IOException {
    assertConsume(nullable, binaryConsumer -> {
      for (byte[] value : values) {
        binaryConsumer.consume(new ByteArrayInputStream(value));
        binaryConsumer.moveWriterPosition();
      }
    }, values);
  }

  @Test
  public void testConsumeInputStream() throws IOException {
    testConsumeInputStream(new byte[][]{
        createBytes(DEFAULT_RECORD_BYTE_COUNT)
    }, false);

    testConsumeInputStream(new byte[][]{
        createBytes(DEFAULT_RECORD_BYTE_COUNT),
        createBytes(DEFAULT_RECORD_BYTE_COUNT)
    }, false);

    testConsumeInputStream(new byte[][]{
        createBytes(DEFAULT_RECORD_BYTE_COUNT * 2),
        createBytes(DEFAULT_RECORD_BYTE_COUNT),
        createBytes(DEFAULT_RECORD_BYTE_COUNT)
    }, false);

    testConsumeInputStream(new byte[][]{
        createBytes(INITIAL_VALUE_ALLOCATION * DEFAULT_RECORD_BYTE_COUNT)
    }, false);

    testConsumeInputStream(new byte[][]{
        createBytes(INITIAL_VALUE_ALLOCATION * DEFAULT_RECORD_BYTE_COUNT * 10),
    }, false);

    testConsumeInputStream(new byte[][]{
        createBytes(INITIAL_VALUE_ALLOCATION * DEFAULT_RECORD_BYTE_COUNT),
        createBytes(INITIAL_VALUE_ALLOCATION * DEFAULT_RECORD_BYTE_COUNT)
    }, false);

    testConsumeInputStream(new byte[][]{
        createBytes(INITIAL_VALUE_ALLOCATION * DEFAULT_RECORD_BYTE_COUNT),
        createBytes(DEFAULT_RECORD_BYTE_COUNT),
        createBytes(INITIAL_VALUE_ALLOCATION * DEFAULT_RECORD_BYTE_COUNT)
    }, false);

    byte[][] testRecords = new byte[INITIAL_VALUE_ALLOCATION * 2][];
    for (int i = 0; i < testRecords.length; i++) {
      testRecords[i] = createBytes(DEFAULT_RECORD_BYTE_COUNT);
    }
    testConsumeInputStream(testRecords, false);
  }

}

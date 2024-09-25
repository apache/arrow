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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.holders.NullableViewVarBinaryHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestViewVarBinaryVector {

  // short string (length <= 12)
  private static final byte[] STR0 = "0123456".getBytes(StandardCharsets.UTF_8);
  // short string (length <= 12)
  private static final byte[] STR1 = "012345678912".getBytes(StandardCharsets.UTF_8);
  // long string (length > 12)
  private static final byte[] STR2 = "0123456789123".getBytes(StandardCharsets.UTF_8);
  // long string (length > 12)
  private static final byte[] STR3 = "01234567891234567".getBytes(StandardCharsets.UTF_8);
  // short string (length <= 12)
  private static final byte[] STR4 = "01234567".getBytes(StandardCharsets.UTF_8);
  // short string (length <= 12)
  private static final byte[] STR5 = "A1234A".getBytes(StandardCharsets.UTF_8);

  private BufferAllocator allocator;

  private Random random;

  @BeforeEach
  public void prepare() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
    random = new Random();
  }

  @AfterEach
  public void shutdown() {
    allocator.close();
  }

  public static void setBytes(int index, byte[] bytes, ViewVarBinaryVector vector) {
    BitVectorHelper.setBit(vector.validityBuffer, index);
    vector.setBytes(index, bytes, 0, bytes.length);
  }

  @Test
  public void testSetNullableViewVarBinaryHolder() {
    try (final ViewVarBinaryVector viewVarBinaryVector =
        new ViewVarBinaryVector("myvector", allocator)) {
      viewVarBinaryVector.allocateNew(0, 0);
      final List<byte[]> strings = List.of(STR0, STR1, STR2, STR3, STR4, STR5);

      NullableViewVarBinaryHolder stringHolder = new NullableViewVarBinaryHolder();

      // set not null
      int size = strings.size();
      for (int i = 0; i < size; i++) {
        setAndCheck(viewVarBinaryVector, i, strings.get(i), stringHolder);
      }

      // set null
      setAndCheck(viewVarBinaryVector, 6, null, stringHolder);

      // copy by holder
      // len < 12
      copyAndCheck(viewVarBinaryVector, 0, stringHolder, 7);
      // len > 12
      copyAndCheck(viewVarBinaryVector, 2, stringHolder, 8);
      // null
      copyAndCheck(viewVarBinaryVector, 6, stringHolder, 9);

      // test overwrite
      for (int i = 0; i < size; i++) {
        setAndCheck(viewVarBinaryVector, i, strings.get(size - i - 1), stringHolder);
      }

      String longString = generateRandomString(128);
      setAndCheck(viewVarBinaryVector, 6, longString.getBytes(), stringHolder);
    }
  }

  private static void copyAndCheck(
      ViewVarBinaryVector viewVarBinaryVector,
      int index,
      NullableViewVarBinaryHolder stringHolder,
      int index1) {
    viewVarBinaryVector.get(index, stringHolder);
    viewVarBinaryVector.setSafe(index1, stringHolder);
    assertArrayEquals(viewVarBinaryVector.get(index), viewVarBinaryVector.get(index1));
  }

  private void setAndCheck(
      ViewVarBinaryVector viewVarBinaryVector,
      int index,
      byte[] str,
      NullableViewVarBinaryHolder stringHolder) {
    ArrowBuf buf = null;
    if (null == str) {
      stringHolder.isSet = 0;
    } else {
      buf = allocator.buffer(str.length);
      buf.setBytes(0, str);
      stringHolder.isSet = 1;
      stringHolder.start = 0;
      stringHolder.end = str.length;
      stringHolder.buffer = buf;
    }

    viewVarBinaryVector.setSafe(index, stringHolder);
    // verify results
    assertArrayEquals(str, viewVarBinaryVector.get(index));
    AutoCloseables.closeNoChecked(buf);
  }

  private String generateRandomString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(random.nextInt(10)); // 0-9
    }
    return sb.toString();
  }
}

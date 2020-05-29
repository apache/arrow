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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.holders.NullableLargeVarBinaryHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLargeVarBinaryVector {

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
  public void testSetNullableLargeVarBinaryHolder() {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("", allocator)) {
      vector.allocateNew(100, 10);

      NullableLargeVarBinaryHolder nullHolder = new NullableLargeVarBinaryHolder();
      nullHolder.isSet = 0;

      NullableLargeVarBinaryHolder binHolder = new NullableLargeVarBinaryHolder();
      binHolder.isSet = 1;

      String str = "hello";
      ArrowBuf buf = allocator.buffer(16);
      buf.setBytes(0, str.getBytes());

      binHolder.start = 0;
      binHolder.end = str.length();
      binHolder.buffer = buf;

      vector.set(0, nullHolder);
      vector.set(1, binHolder);

      // verify results
      assertTrue(vector.isNull(0));
      assertEquals(str, new String(vector.get(1)));

      buf.close();
    }
  }

  @Test
  public void testSetNullableLargeVarBinaryHolderSafe() {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("", allocator)) {
      vector.allocateNew(5, 1);

      NullableLargeVarBinaryHolder nullHolder = new NullableLargeVarBinaryHolder();
      nullHolder.isSet = 0;

      NullableLargeVarBinaryHolder binHolder = new NullableLargeVarBinaryHolder();
      binHolder.isSet = 1;

      String str = "hello world";
      ArrowBuf buf = allocator.buffer(16);
      buf.setBytes(0, str.getBytes());

      binHolder.start = 0;
      binHolder.end = str.length();
      binHolder.buffer = buf;

      vector.setSafe(0, binHolder);
      vector.setSafe(1, nullHolder);

      // verify results
      assertEquals(str, new String(vector.get(0)));
      assertTrue(vector.isNull(1));

      buf.close();
    }
  }
}

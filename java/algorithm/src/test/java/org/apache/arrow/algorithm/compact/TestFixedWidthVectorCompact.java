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

package org.apache.arrow.algorithm.compact;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFixedWidthVectorCompact {

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testInt() {
    try (IntVector vec = new IntVector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      vec.set(0, 10);
      vec.setNull(1);
      vec.setNull(2);
      vec.setNull(3);
      vec.set(4, 20);
      vec.setNull(5);
      vec.set(6, 30);
      vec.set(7, 40);
      vec.setNull(8);
      vec.setNull(9);

      BitVector bitVector = VectorDataCompactor.compact(vec);

      assertEquals(4, vec.getValueCount());
      assertEquals(6, bitVector.getNullCount());
      assertEquals(10, vec.get(0));
      assertEquals(20, vec.get(1));
      assertEquals(30, vec.get(2));
      assertEquals(40, vec.get(3));


      IntVector resVec = VectorDataCompactor.uncompact(bitVector, vec);

      assertEquals(10, resVec.getValueCount());
      assertEquals(10, resVec.get(0));
      assertTrue(resVec.isNull(1));
      assertTrue(resVec.isNull(2));
      assertTrue(resVec.isNull(3));
      assertEquals(20, resVec.get(4));
      assertTrue(resVec.isNull(5));
      assertEquals(30, resVec.get(6));
      assertEquals(40, resVec.get(7));
      assertTrue(resVec.isNull(8));
      assertTrue(resVec.isNull(9));

      bitVector.close();
      resVec.close();
    }
  }

}

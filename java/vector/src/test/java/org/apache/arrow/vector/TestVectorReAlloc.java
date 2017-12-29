/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestVectorReAlloc {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testFixedType() {
    try (final UInt4Vector vector = new UInt4Vector("", allocator)) {
      vector.setInitialCapacity(512);
      vector.allocateNew();

      assertEquals(512, vector.getValueCapacity());

      try {
        vector.set(512, 0);
        Assert.fail("Expected out of bounds exception");
      } catch (Exception e) {
        // ok
      }

      vector.reAlloc();
      assertEquals(1024, vector.getValueCapacity());

      vector.set(512, 100);
      assertEquals(100, vector.get(512));
    }
  }

  @Test
  public void testNullableType() {
    try (final VarCharVector vector = new VarCharVector("", allocator)) {
      vector.setInitialCapacity(512);
      vector.allocateNew();

      assertEquals(512, vector.getValueCapacity());

      try {
        vector.set(512, "foo".getBytes(StandardCharsets.UTF_8));
        Assert.fail("Expected out of bounds exception");
      } catch (Exception e) {
        // ok
      }

      vector.reAlloc();
      assertEquals(1024, vector.getValueCapacity());

      vector.set(512, "foo".getBytes(StandardCharsets.UTF_8));
      assertEquals("foo", new String(vector.get(512), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testListType() {
    try (final ListVector vector = ListVector.empty("", allocator)) {
      vector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));

      vector.setInitialCapacity(512);
      vector.allocateNew();

      assertEquals(1023, vector.getValueCapacity());

      try {
        vector.getInnerValueCountAt(2014);
        Assert.fail("Expected out of bounds exception");
      } catch (Exception e) {
        // ok
      }

      vector.reAlloc();
      assertEquals(2047, vector.getValueCapacity()); // note: size - 1
      assertEquals(0, vector.getOffsetBuffer().getInt(2014 * ListVector.OFFSET_WIDTH));
    }
  }

  @Test
  public void testMapType() {
    try (final NullableMapVector vector = NullableMapVector.empty("", allocator)) {
      vector.addOrGet("", FieldType.nullable(MinorType.INT.getType()), IntVector.class);

      vector.setInitialCapacity(512);
      vector.allocateNew();

      assertEquals(512, vector.getValueCapacity());

      try {
        vector.getObject(513);
        Assert.fail("Expected out of bounds exception");
      } catch (Exception e) {
        // ok
      }

      vector.reAlloc();
      assertEquals(1024, vector.getValueCapacity());
      assertNull(vector.getObject(513));
    }
  }
}

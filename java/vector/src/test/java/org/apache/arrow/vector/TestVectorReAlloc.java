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

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.DataSizeRoundingUtil;
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

      assertTrue(vector.getValueCapacity() >= 512);
      int initialCapacity = vector.getValueCapacity();

      try {
        vector.set(initialCapacity, 0);
        Assert.fail("Expected out of bounds exception");
      } catch (Exception e) {
        // ok
      }

      vector.reAlloc();
      assertTrue(vector.getValueCapacity() >= 2 * initialCapacity);

      vector.set(initialCapacity, 100);
      assertEquals(100, vector.get(initialCapacity));
    }
  }

  @Test
  public void testNullableType() {
    try (final VarCharVector vector = new VarCharVector("", allocator)) {
      vector.setInitialCapacity(512);
      vector.allocateNew();

      assertTrue(vector.getValueCapacity() >= 512);
      int initialCapacity = vector.getValueCapacity();

      try {
        vector.set(initialCapacity, "foo".getBytes(StandardCharsets.UTF_8));
        Assert.fail("Expected out of bounds exception");
      } catch (Exception e) {
        // ok
      }

      vector.reAlloc();
      assertTrue(vector.getValueCapacity() >= 2 * initialCapacity);

      vector.set(initialCapacity, "foo".getBytes(StandardCharsets.UTF_8));
      assertEquals("foo", new String(vector.get(initialCapacity), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testListType() {
    try (final ListVector vector = ListVector.empty("", allocator)) {
      vector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));

      vector.setInitialCapacity(512);
      vector.allocateNew();

      assertEquals(512, vector.getValueCapacity());

      try {
        vector.getInnerValueCountAt(2014);
        Assert.fail("Expected out of bounds exception");
      } catch (Exception e) {
        // ok
      }

      vector.reAlloc();
      assertEquals(1024, vector.getValueCapacity());
      assertEquals(0, vector.getOffsetBuffer().getInt(2014 * ListVector.OFFSET_WIDTH));
    }
  }

  @Test
  public void testStructType() {
    try (final StructVector vector = StructVector.empty("", allocator)) {
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

  @Test
  public void testVariableWidthTypeSetNullValues() {
    // Test ARROW-11223 bug is fixed
    try (final BaseVariableWidthVector v1 = new VarCharVector("var1", allocator)) {
      v1.setInitialCapacity(512);
      v1.allocateNew();
      int numNullValues1 = v1.getValueCapacity() + 1;
      for (int i = 0; i < numNullValues1; i++) {
        v1.setNull(i);
      }
      Assert.assertTrue(v1.getBufferSizeFor(numNullValues1) > 0);
    }

    try (final BaseLargeVariableWidthVector v2 = new LargeVarCharVector("var2", allocator)) {
      v2.setInitialCapacity(512);
      v2.allocateNew();
      int numNullValues2 = v2.getValueCapacity() + 1;
      for (int i = 0; i < numNullValues2; i++) {
        v2.setNull(i);
      }
      Assert.assertTrue(v2.getBufferSizeFor(numNullValues2) > 0);
    }
  }

  @Test
  public void testFixedAllocateAfterReAlloc() throws Exception {
    try (final IntVector vector = new IntVector("", allocator)) {
      /*
       * Allocate the default size, and then, reAlloc. This should double the allocation.
       */
      vector.allocateNewSafe(); // Initial allocation
      vector.reAlloc(); // Double the allocation size.
      int savedValueCapacity = vector.getValueCapacity();

      /*
       * Clear and allocate again.
       */
      vector.clear();
      vector.allocateNewSafe();

      /*
       * Verify that the buffer sizes haven't changed.
       */
      Assert.assertEquals(vector.getValueCapacity(), savedValueCapacity);
    }
  }

  @Test
  public void testVariableAllocateAfterReAlloc() throws Exception {
    try (final VarCharVector vector = new VarCharVector("", allocator)) {
      /*
       * Allocate the default size, and then, reAlloc. This should double the allocation.
       */
      vector.allocateNewSafe(); // Initial allocation
      vector.reAlloc(); // Double the allocation size.
      int savedValueCapacity = vector.getValueCapacity();
      long savedValueBufferSize = vector.valueBuffer.capacity();

      /*
       * Clear and allocate again.
       */
      vector.clear();
      vector.allocateNewSafe();

      /*
       * Verify that the buffer sizes haven't changed.
       */
      Assert.assertEquals(vector.getValueCapacity(), savedValueCapacity);
      Assert.assertEquals(vector.valueBuffer.capacity(), savedValueBufferSize);
    }
  }

  @Test
  public void testLargeVariableAllocateAfterReAlloc() throws Exception {
    try (final LargeVarCharVector vector = new LargeVarCharVector("", allocator)) {
      /*
       * Allocate the default size, and then, reAlloc. This should double the allocation.
       */
      vector.allocateNewSafe(); // Initial allocation
      vector.reAlloc(); // Double the allocation size.
      int savedValueCapacity = vector.getValueCapacity();
      long savedValueBufferSize = vector.valueBuffer.capacity();

      /*
       * Clear and allocate again.
       */
      vector.clear();
      vector.allocateNewSafe();

      /*
       * Verify that the buffer sizes haven't changed.
       */
      Assert.assertEquals(vector.getValueCapacity(), savedValueCapacity);
      Assert.assertEquals(vector.valueBuffer.capacity(), savedValueBufferSize);
    }
  }

  @Test
  public void testVarCharAllocateNew() throws Exception {
    final int count = 6000;

    try (final VarCharVector vector = new VarCharVector("", allocator)) {
      vector.allocateNew(count);
      
      // verify that the validity buffer and value buffer have capacity for at least 'count' elements.
      Assert.assertTrue(vector.getValidityBuffer().capacity() >= DataSizeRoundingUtil.divideBy8Ceil(count));
      Assert.assertTrue(vector.getOffsetBuffer().capacity() >= (count + 1) * BaseVariableWidthVector.OFFSET_WIDTH);
    }
  }

  @Test
  public void testLargeVarCharAllocateNew() throws Exception {
    final int count = 6000;

    try (final LargeVarCharVector vector = new LargeVarCharVector("", allocator)) {
      vector.allocateNew(count);
      
      // verify that the validity buffer and value buffer have capacity for at least 'count' elements.
      Assert.assertTrue(vector.getValidityBuffer().capacity() >= DataSizeRoundingUtil.divideBy8Ceil(count));
      Assert.assertTrue(vector.getOffsetBuffer().capacity() >= (count + 1) * BaseLargeVariableWidthVector.OFFSET_WIDTH);
    }
  }

  @Test
  public void testVarCharAllocateNewUsingHelper() throws Exception {
    final int count = 6000;

    try (final VarCharVector vector = new VarCharVector("", allocator)) {
      AllocationHelper.allocateNew(vector, count);

      // verify that the validity buffer and value buffer have capacity for at least 'count' elements.
      Assert.assertTrue(vector.getValidityBuffer().capacity() >= DataSizeRoundingUtil.divideBy8Ceil(count));
      Assert.assertTrue(vector.getOffsetBuffer().capacity() >= (count + 1) * BaseVariableWidthVector.OFFSET_WIDTH);
    }
  }

  @Test
  public void testLargeVarCharAllocateNewUsingHelper() throws Exception {
    final int count = 6000;

    try (final LargeVarCharVector vector = new LargeVarCharVector("", allocator)) {
      AllocationHelper.allocateNew(vector, count);

      // verify that the validity buffer and value buffer have capacity for at least 'count' elements.
      Assert.assertTrue(vector.getValidityBuffer().capacity() >= DataSizeRoundingUtil.divideBy8Ceil(count));
      Assert.assertTrue(vector.getOffsetBuffer().capacity() >= (count + 1) * BaseLargeVariableWidthVector.OFFSET_WIDTH);
    }
  }

  @Test
  public void testFixedRepeatedClearAndSet() throws Exception {
    try (final IntVector vector = new IntVector("", allocator)) {
      vector.allocateNewSafe(); // Initial allocation
      vector.clear(); // clear vector.
      vector.setSafe(0, 10);
      int savedValueCapacity = vector.getValueCapacity();

      for (int i = 0; i < 1024; ++i) {
        vector.clear(); // clear vector.
        vector.setSafe(0, 10);
      }

      // should be deterministic, and not cause a run-away increase in capacity.
      Assert.assertEquals(vector.getValueCapacity(), savedValueCapacity);
    }
  }

  @Test
  public void testVariableRepeatedClearAndSet() throws Exception {
    try (final VarCharVector vector = new VarCharVector("", allocator)) {
      vector.allocateNewSafe(); // Initial allocation

      vector.clear(); // clear vector.
      vector.setSafe(0, "hello world".getBytes(StandardCharsets.UTF_8));
      int savedValueCapacity = vector.getValueCapacity();

      for (int i = 0; i < 1024; ++i) {
        vector.clear(); // clear vector.
        vector.setSafe(0, "hello world".getBytes(StandardCharsets.UTF_8));
      }

      // should be deterministic, and not cause a run-away increase in capacity.
      Assert.assertEquals(vector.getValueCapacity(), savedValueCapacity);
    }
  }

  @Test
  public void testRepeatedValueVectorClearAndSet() throws Exception {
    try (final ListVector vector = new ListVector("", allocator, FieldType.nullable(MinorType.INT.getType()), null)) {
      vector.allocateNewSafe(); // Initial allocation
      UnionListWriter writer = vector.getWriter();

      vector.clear(); // clear vector.
      writer.setPosition(0); // optional
      writer.startList();
      writer.writeInt(0);
      writer.endList();
      int savedValueCapacity = vector.getValueCapacity();

      for (int i = 0; i < 1024; ++i) {
        vector.clear(); // clear vector.
        writer.setPosition(0); // optional
        writer.startList();
        writer.writeInt(i);
        writer.endList();
      }

      // should be deterministic, and not cause a run-away increase in capacity.
      Assert.assertEquals(vector.getValueCapacity(), savedValueCapacity);
    }
  }

  @Test
  public void testStructVectorClearAndSet() throws Exception {
    try (final StructVector vector = StructVector.empty("v", allocator)) {
      vector.allocateNewSafe(); // Initial allocation

      NullableStructWriter writer = vector.getWriter();

      vector.clear(); // clear vector.
      writer.setPosition(0); // optional
      writer.start();
      writer.integer("int").writeInt(0);
      writer.end();
      int savedValueCapacity = vector.getValueCapacity();

      for (int i = 0; i < 1024; ++i) {
        vector.clear(); // clear vector.
        writer.setPosition(0); // optional
        writer.start();
        writer.integer("int").writeInt(i);
        writer.end();
      }

      // should be deterministic, and not cause a run-away increase in capacity.
      Assert.assertEquals(vector.getValueCapacity(), savedValueCapacity);
    }
  }

  @Test
  public void testFixedSizeListVectorClearAndSet() {
    try (final FixedSizeListVector vector = new FixedSizeListVector("", allocator,
        FieldType.nullable(new ArrowType.FixedSizeList(2)), null)) {
      vector.allocateNewSafe(); // Initial allocation
      UnionFixedSizeListWriter writer = vector.getWriter();

      vector.clear(); // clear vector.
      writer.setPosition(0); // optional
      writer.startList();
      writer.writeInt(0);
      writer.writeInt(1);
      writer.endList();
      int savedValueCapacity = vector.getValueCapacity();

      for (int i = 0; i < 1024; ++i) {
        vector.clear(); // clear vector.
        writer.setPosition(0); // optional
        writer.startList();
        writer.writeInt(i);
        writer.writeInt(i + 1);
        writer.endList();
      }

      // should be deterministic, and not cause a run-away increase in capacity.
      Assert.assertEquals(vector.getValueCapacity(), savedValueCapacity);
    }
  }

  @Test
  public void testUnionVectorClearAndSet() {
    try (final UnionVector vector = new UnionVector("", allocator, /* field type */ null, /* call-back */ null)) {
      vector.allocateNewSafe(); // Initial allocation

      NullableIntHolder holder = new NullableIntHolder();
      holder.isSet = 1;
      holder.value = 1;

      vector.clear(); // clear vector.
      vector.setType(0, MinorType.INT);
      vector.setSafe(0, holder);
      int savedValueCapacity = vector.getValueCapacity();

      for (int i = 0; i < 1024; ++i) {
        vector.clear(); // clear vector.
        vector.setType(0, MinorType.INT);
        vector.setSafe(0, holder);
      }

      // should be deterministic, and not cause a run-away increase in capacity.
      Assert.assertEquals(vector.getValueCapacity(), savedValueCapacity);
    }
  }

  @Test
  public void testDenseUnionVectorClearAndSet() {
    try (final DenseUnionVector vector = new DenseUnionVector("", allocator, null, null)) {
      vector.allocateNewSafe(); // Initial allocation

      NullableIntHolder holder = new NullableIntHolder();
      holder.isSet = 1;
      holder.value = 1;

      byte intTypeId = vector.registerNewTypeId(Field.nullable("", MinorType.INT.getType()));

      vector.clear();
      vector.setTypeId(0, intTypeId);
      vector.setSafe(0, holder);

      int savedValueCapacity = vector.getValueCapacity();

      for (int i = 0; i < 1024; ++i) {
        vector.clear();
        vector.setTypeId(0, intTypeId);
        vector.setSafe(0, holder);
      }

      // should be deterministic, and not cause a run-away increase in capacity.
      Assert.assertEquals(vector.getValueCapacity(), savedValueCapacity);
    }
  }
}

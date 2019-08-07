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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.holders.ComplexHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.FieldType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestStructVector {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testFieldMetadata() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("k1", "v1");
    FieldType type = new FieldType(true, Struct.INSTANCE, null, metadata);
    try (StructVector vector = new StructVector("struct", allocator, type, null)) {
      Assert.assertEquals(vector.getField().getMetadata(), type.getMetadata());
    }
  }

  @Test
  public void testMakeTransferPair() {
    try (final StructVector s1 = StructVector.empty("s1", allocator);
         final StructVector s2 = StructVector.empty("s2", allocator)) {
      s1.addOrGet("struct_child", FieldType.nullable(MinorType.INT.getType()), IntVector.class);
      s1.makeTransferPair(s2);
      final FieldVector child = s1.getChild("struct_child");
      final FieldVector toChild = s2.addOrGet("struct_child", child.getField().getFieldType(), child.getClass());
      assertEquals(0, toChild.getValueCapacity());
      assertEquals(0, toChild.getDataBuffer().capacity());
      assertEquals(0, toChild.getValidityBuffer().capacity());
    }
  }

  @Test
  public void testAllocateAfterReAlloc() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("k1", "v1");
    FieldType type = new FieldType(true, Struct.INSTANCE, null, metadata);
    try (StructVector vector = new StructVector("struct", allocator, type, null)) {
      MinorType childtype = MinorType.INT;
      vector.addOrGet("intchild", FieldType.nullable(childtype.getType()), IntVector.class);

      /*
       * Allocate the default size, and then, reAlloc. This should double the allocation.
       */
      vector.allocateNewSafe(); // Initial allocation
      vector.reAlloc(); // Double the allocation size of self, and all children.
      int savedValidityBufferCapacity = vector.getValidityBuffer().capacity();
      int savedValueCapacity = vector.getValueCapacity();

      /*
       * Clear and allocate again.
       */
      vector.clear();
      vector.allocateNewSafe();

      /*
       * Verify that the buffer sizes haven't changed.
       */
      Assert.assertEquals(vector.getValidityBuffer().capacity(), savedValidityBufferCapacity);
      Assert.assertEquals(vector.getValueCapacity(), savedValueCapacity);
    }
  }

  @Test
  public void testReadNullValue() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("k1", "v1");
    FieldType type = new FieldType(true, Struct.INSTANCE, null, metadata);
    try (StructVector vector = new StructVector("struct", allocator, type, null)) {
      MinorType childtype = MinorType.INT;
      vector.addOrGet("intchild", FieldType.nullable(childtype.getType()), IntVector.class);
      vector.setValueCount(2);

      IntVector intVector = (IntVector) vector.getChild("intchild");
      intVector.setSafe(0, 100);
      vector.setIndexDefined(0);
      intVector.setNull(1);
      vector.setNull(1);

      ComplexHolder holder = new ComplexHolder();
      vector.get(0, holder);
      assertNotEquals(0, holder.isSet);
      assertNotNull(holder.reader);

      vector.get(1, holder);
      assertEquals(0, holder.isSet);
      assertNull(holder.reader);
    }
  }

  @Test
  public void testGetPrimitiveVectors() {
    FieldType type = new FieldType(true, Struct.INSTANCE, null, null);
    try (StructVector vector = new StructVector("struct", allocator, type, null)) {

      // add list vector
      vector.addOrGet("list", FieldType.nullable(MinorType.LIST.getType()), ListVector.class);
      ListVector listVector = vector.addOrGetList("list");
      listVector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));

      // add union vector
      vector.addOrGet("union", FieldType.nullable(MinorType.UNION.getType()), UnionVector.class);
      UnionVector unionVector = vector.addOrGetUnion("union");
      unionVector.addVector(new BigIntVector("bigInt", allocator));
      unionVector.addVector(new SmallIntVector("smallInt", allocator));

      // add varchar vector
      vector.addOrGet("varchar", FieldType.nullable(MinorType.VARCHAR.getType()), VarCharVector.class);

      List<ValueVector> primitiveVectors = vector.getPrimitiveVectors();
      assertEquals(4, primitiveVectors.size());
      assertEquals(MinorType.INT, primitiveVectors.get(0).getMinorType());
      assertEquals(MinorType.BIGINT, primitiveVectors.get(1).getMinorType());
      assertEquals(MinorType.SMALLINT, primitiveVectors.get(2).getMinorType());
      assertEquals(MinorType.VARCHAR, primitiveVectors.get(3).getMinorType());
    }
  }
}

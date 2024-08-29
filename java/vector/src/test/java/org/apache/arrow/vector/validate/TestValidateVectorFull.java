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

package org.apache.arrow.vector.validate;

import static org.apache.arrow.vector.testing.ValueVectorDataPopulator.setVector;
import static org.apache.arrow.vector.util.ValueVectorUtility.validateFull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestValidateVectorFull {

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
  public void testBaseVariableWidthVector() {
    try (final VarCharVector vector = new VarCharVector("v", allocator)) {
      validateFull(vector);
      setVector(vector, "aaa", "bbb", "ccc");
      validateFull(vector);

      ArrowBuf offsetBuf = vector.getOffsetBuffer();
      offsetBuf.setInt(0, 100);
      offsetBuf.setInt(4, 50);

      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          () -> validateFull(vector));
      assertTrue(e.getMessage().contains("The values in positions 0 and 1 of the offset buffer are decreasing"));
    }
  }

  @Test
  public void testBaseLargeVariableWidthVector() {
    try (final LargeVarCharVector vector = new LargeVarCharVector("v", allocator)) {
      validateFull(vector);
      setVector(vector, "aaa", "bbb", null, "ccc");
      validateFull(vector);

      ArrowBuf offsetBuf = vector.getOffsetBuffer();
      offsetBuf.setLong(0, 100);
      offsetBuf.setLong(8, 50);

      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          () -> validateFull(vector));
      assertTrue(e.getMessage().contains("The values in positions 0 and 1 of the large offset buffer are decreasing"));
    }
  }

  @Test
  public void testListVector() {
    try (final ListVector vector = ListVector.empty("v", allocator)) {
      validateFull(vector);
      setVector(vector, Arrays.asList(1, 2, 3), Arrays.asList(4, 5), Arrays.asList(6, 7, 8, 9));
      validateFull(vector);

      ArrowBuf offsetBuf = vector.getOffsetBuffer();
      offsetBuf.setInt(0, 100);
      offsetBuf.setInt(8, 50);

      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          () -> validateFull(vector));
      assertTrue(e.getMessage().contains("The values in positions 0 and 1 of the offset buffer are decreasing"));
    }
  }

  @Test
  public void testLargeListVector() {
    try (final LargeListVector vector = LargeListVector.empty("v", allocator)) {
      validateFull(vector);
      setVector(vector, Arrays.asList(1, 2, 3), Arrays.asList(4, 5), Arrays.asList(6, 7, 8, 9));
      validateFull(vector);

      ArrowBuf offsetBuf = vector.getOffsetBuffer();
      offsetBuf.setLong(0, 100);
      offsetBuf.setLong(16, 50);

      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          () -> validateFull(vector));
      assertTrue(e.getMessage().contains("The values in positions 0 and 1 of the large offset buffer are decreasing"));
    }
  }

  @Test
  public void testStructVectorRangeEquals() {
    try (final StructVector vector = StructVector.empty("struct", allocator)) {
      IntVector intVector =
          vector.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      VarCharVector strVector =
          vector.addOrGet("f1", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);

      validateFull(vector);
      validateFull(intVector);
      validateFull(strVector);

      ValueVectorDataPopulator.setVector(intVector, 1, 2, 3, 4, 5);
      ValueVectorDataPopulator.setVector(strVector, "a", "b", "c", "d", "e");
      vector.setValueCount(5);

      validateFull(vector);
      validateFull(intVector);
      validateFull(strVector);

      ArrowBuf offsetBuf = strVector.getOffsetBuffer();
      offsetBuf.setInt(0, 100);
      offsetBuf.setInt(8, 50);

      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          () -> validateFull(strVector));
      assertTrue(e.getMessage().contains("The values in positions 0 and 1 of the offset buffer are decreasing"));

      e = assertThrows(ValidateUtil.ValidateException.class,
          () -> validateFull(vector));
      assertTrue(e.getMessage().contains("The values in positions 0 and 1 of the offset buffer are decreasing"));
    }
  }

  @Test
  public void testUnionVector() {
    try (final UnionVector vector = UnionVector.empty("union", allocator)) {
      validateFull(vector);

      final NullableFloat4Holder float4Holder = new NullableFloat4Holder();
      float4Holder.value = 1.01f;
      float4Holder.isSet = 1;

      final NullableFloat8Holder float8Holder = new NullableFloat8Holder();
      float8Holder.value = 2.02f;
      float8Holder.isSet = 1;

      vector.setType(0, Types.MinorType.FLOAT4);
      vector.setSafe(0, float4Holder);
      vector.setType(1, Types.MinorType.FLOAT8);
      vector.setSafe(1, float8Holder);
      vector.setValueCount(2);

      validateFull(vector);

      // negative type id
      vector.getTypeBuffer().setByte(0, -1);

      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          () -> validateFull(vector));
      assertTrue(e.getMessage().contains("The type id at position 0 is negative"));
    }
  }

  @Test
  public void testDenseUnionVector() {
    try (final DenseUnionVector vector = DenseUnionVector.empty("union", allocator)) {
      validateFull(vector);

      final NullableFloat4Holder float4Holder = new NullableFloat4Holder();
      float4Holder.value = 1.01f;
      float4Holder.isSet = 1;

      final NullableFloat8Holder float8Holder = new NullableFloat8Holder();
      float8Holder.value = 2.02f;
      float8Holder.isSet = 1;

      byte float4TypeId = vector.registerNewTypeId(Field.nullable("", Types.MinorType.FLOAT4.getType()));
      byte float8TypeId = vector.registerNewTypeId(Field.nullable("", Types.MinorType.FLOAT8.getType()));

      vector.setTypeId(0, float4TypeId);
      vector.setSafe(0, float4Holder);
      vector.setTypeId(1, float8TypeId);
      vector.setSafe(1, float8Holder);
      vector.setValueCount(2);

      validateFull(vector);

      ValueVector subVector = vector.getVectorByType(float4TypeId);
      assertTrue(subVector instanceof Float4Vector);
      assertEquals(1, subVector.getValueCount());

      // shrink sub-vector
      subVector.setValueCount(0);

      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          () -> validateFull(vector));
      assertTrue(e.getMessage().contains("Dense union vector offset exceeds sub-vector boundary"));
    }
  }

  @Test
  public void testBaseVariableWidthVectorInstanceMethod() {
    try (final VarCharVector vector = new VarCharVector("v", allocator)) {
      vector.validateFull();
      setVector(vector, "aaa", "bbb", "ccc");
      vector.validateFull();

      ArrowBuf offsetBuf = vector.getOffsetBuffer();
      offsetBuf.setInt(0, 100);
      offsetBuf.setInt(4, 50);

      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          vector::validateFull);
      assertTrue(e.getMessage().contains("The values in positions 0 and 1 of the offset buffer are decreasing"));
    }
  }

  @Test
  public void testValidateVarCharUTF8() {
    try (final VarCharVector vector = new VarCharVector("v", allocator)) {
      vector.validateFull();
      setVector(vector, "aaa".getBytes(StandardCharsets.UTF_8), "bbb".getBytes(StandardCharsets.UTF_8),
          new byte[] {(byte) 0xFF, (byte) 0xFE});
      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          vector::validateFull);
      assertTrue(e.getMessage().contains("UTF"));
    }
  }

  @Test
  public void testValidateLargeVarCharUTF8() {
    try (final LargeVarCharVector vector = new LargeVarCharVector("v", allocator)) {
      vector.validateFull();
      setVector(vector, "aaa".getBytes(StandardCharsets.UTF_8), "bbb".getBytes(StandardCharsets.UTF_8),
          new byte[] {(byte) 0xFF, (byte) 0xFE});
      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          vector::validateFull);
      assertTrue(e.getMessage().contains("UTF"));
    }
  }

  @Test
  public void testValidateDecimal() {
    try (final DecimalVector vector = new DecimalVector(Field.nullable("v",
        new ArrowType.Decimal(2, 0, DecimalVector.TYPE_WIDTH * 8)), allocator)) {
      vector.validateFull();
      setVector(vector, 1L);
      vector.validateFull();
      vector.clear();
      setVector(vector, Long.MAX_VALUE);
      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          vector::validateFull);
      assertTrue(e.getMessage().contains("Decimal"));
    }
  }

  @Test
  public void testValidateDecimal256() {
    try (final Decimal256Vector vector = new Decimal256Vector(Field.nullable("v",
        new ArrowType.Decimal(2, 0, DecimalVector.TYPE_WIDTH * 8)), allocator)) {
      vector.validateFull();
      setVector(vector, 1L);
      vector.validateFull();
      vector.clear();
      setVector(vector, Long.MAX_VALUE);
      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          vector::validateFull);
      assertTrue(e.getMessage().contains("Decimal"));
    }
  }
}

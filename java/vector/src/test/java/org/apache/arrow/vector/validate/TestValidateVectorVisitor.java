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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestValidateVectorVisitor {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  private static final Charset utf8Charset = Charset.forName("UTF-8");
  private static final byte[] STR1 = "AAAAA1".getBytes(utf8Charset);
  private static final byte[] STR2 = "BBBBBBBBB2".getBytes(utf8Charset);
  private static final byte[] STR3 = "CCCC3".getBytes(utf8Charset);

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testBaseFixedWidthVector() {
    try (final IntVector vector = new IntVector("v", allocator)) {
      assertTrue(vector.validate().isSuccess());
      setVector(vector, 1, 2, 3);
      assertTrue(vector.validate().isSuccess());

      vector.getDataBuffer().capacity(0);
      assertFalse(vector.validate().isSuccess());
      assertTrue(vector.validate().getMessage().contains("Buffer #1 too small in vector of type"));
    }
  }

  @Test
  public void testBaseVariableWidthVector() {
    try (final VarCharVector vector = new VarCharVector("v", allocator)) {
      assertTrue(vector.validate().isSuccess());
      setVector(vector, STR1, STR2, STR3);
      assertTrue(vector.validate().isSuccess());

      vector.getDataBuffer().capacity(0);
      assertFalse(vector.validate().isSuccess());
      assertTrue(vector.validate().getMessage().contains("Buffer #2 too small in vector of type"));
    }
  }

  @Test
  public void testListVector() {
    try (final ListVector vector = ListVector.empty("v", allocator)) {
      assertTrue(vector.validate().isSuccess());
      setVector(vector, Arrays.asList(1,2,3), Arrays.asList(4, 5));
      assertTrue(vector.validate().isSuccess());

      vector.getDataVector().setValueCount(3);
      assertFalse(vector.validate().isSuccess());
      System.out.println(vector.validate().toString());
      assertEquals("Length spanned by list offsets (5) larger than data vector valueCount (length 3)",
          vector.validate().getMessage());
    }
  }

  @Test
  public void testFixedSizeListVector() {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("v", 3, allocator)) {
      assertTrue(vector.validate().isSuccess());
      setVector(vector, Arrays.asList(1,2,3), Arrays.asList(4, 5, 6));
      assertTrue(vector.validate().isSuccess());

      vector.getDataVector().setValueCount(3);
      assertFalse(vector.validate().isSuccess());
      System.out.println(vector.validate().toString());
      assertEquals("data vector valueCount invalid, expect 6, actual is: 3", vector.validate().getMessage());
    }
  }

  @Test
  public void testStructVectorRangeEquals() {
    try (final StructVector vector = StructVector.empty("struct", allocator)) {
      vector.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);

      assertTrue(vector.validate().isSuccess());

      NullableStructWriter writer = vector.getWriter();
      writer.allocate();

      writeStructVector(writer, 1, 10L);
      writeStructVector(writer, 2, 20L);
      writeStructVector(writer, 3, 30L);
      writeStructVector(writer, 4, 40L);
      writeStructVector(writer, 5, 50L);
      writer.setValueCount(5);

      vector.getChild("f0").setValueCount(2);
      assertFalse(vector.validate().isSuccess());
      assertTrue(vector.validate().toString().contains("valueCount is not equals with struct vector"));

      vector.getChild("f0").setValueCount(5);
      assertTrue(vector.validate().isSuccess());

      vector.getChild("f0").getDataBuffer().capacity(0);
      assertFalse(vector.validate().isSuccess());
      assertTrue(vector.validate().toString().contains("struct child vector invalid"));
    }
  }

  @Test
  public void testUnionVector() {
    try (final UnionVector vector = UnionVector.empty("union", allocator)) {
      assertTrue(vector.validate().isSuccess());

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

      assertTrue(vector.validate().isSuccess());

      vector.getChildrenFromFields().get(0).setValueCount(1);
      assertFalse(vector.validate().isSuccess());
      assertTrue(vector.validate().toString().contains("valueCount is not equals with union vector"));

      vector.getChildrenFromFields().get(0).setValueCount(2);
      assertTrue(vector.validate().isSuccess());

      vector.getChildrenFromFields().get(0).getDataBuffer().capacity(0);
      assertFalse(vector.validate().isSuccess());
      assertTrue(vector.validate().toString().contains("union child vector invalid"));
    }
  }

  @Test
  public void testDenseUnionVector() {
    try (final DenseUnionVector vector = DenseUnionVector.empty("union", allocator)) {
      assertTrue(vector.validate().isSuccess());

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

      assertTrue(vector.validate().isSuccess());

      vector.getChildrenFromFields().get(0).getDataBuffer().capacity(0);
      assertFalse(vector.validate().isSuccess());
      assertTrue(vector.validate().toString().contains("union child vector invalid"));
    }
  }

  private void writeStructVector(NullableStructWriter writer, int value1, long value2) {
    writer.start();
    writer.integer("f0").writeInt(value1);
    writer.bigInt("f1").writeBigInt(value2);
    writer.end();
  }


}

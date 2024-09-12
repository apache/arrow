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
import static org.apache.arrow.vector.util.ValueVectorUtility.validate;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.RunEndEncodedVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.RunEndEncoded;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestValidateVector {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  private static final Charset utf8Charset = Charset.forName("UTF-8");
  private static final byte[] STR1 = "AAAAA1".getBytes(utf8Charset);
  private static final byte[] STR2 = "BBBBBBBBB2".getBytes(utf8Charset);
  private static final byte[] STR3 = "CCCC3".getBytes(utf8Charset);

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testBaseFixedWidthVector() {
    try (final IntVector vector = new IntVector("v", allocator)) {
      validate(vector);
      setVector(vector, 1, 2, 3);
      validate(vector);

      vector.getDataBuffer().capacity(0);
      ValidateUtil.ValidateException e =
          assertThrows(ValidateUtil.ValidateException.class, () -> validate(vector));
      assertTrue(e.getMessage().contains("Not enough capacity for fixed width data buffer"));
    }
  }

  @Test
  public void testBaseVariableWidthVector() {
    try (final VarCharVector vector = new VarCharVector("v", allocator)) {
      validate(vector);
      setVector(vector, STR1, STR2, STR3);
      validate(vector);

      vector.getDataBuffer().capacity(0);
      ValidateUtil.ValidateException e =
          assertThrows(ValidateUtil.ValidateException.class, () -> validate(vector));
      assertTrue(e.getMessage().contains("Not enough capacity for data buffer"));
    }
  }

  @Test
  public void testBaseLargeVariableWidthVector() {
    try (final LargeVarCharVector vector = new LargeVarCharVector("v", allocator)) {
      validate(vector);
      setVector(vector, STR1, STR2, null, STR3);
      validate(vector);

      vector.getDataBuffer().capacity(0);
      ValidateUtil.ValidateException e =
          assertThrows(ValidateUtil.ValidateException.class, () -> validate(vector));
      assertTrue(e.getMessage().contains("Not enough capacity for data buffer"));
    }
  }

  @Test
  public void testListVector() {
    try (final ListVector vector = ListVector.empty("v", allocator)) {
      validate(vector);
      setVector(vector, Arrays.asList(1, 2, 3), Arrays.asList(4, 5));
      validate(vector);

      vector.getDataVector().setValueCount(3);
      ValidateUtil.ValidateException e =
          assertThrows(ValidateUtil.ValidateException.class, () -> validate(vector));
      assertTrue(e.getMessage().contains("Inner vector does not contain enough elements."));
    }
  }

  @Test
  public void testLargeListVector() {
    try (final LargeListVector vector = LargeListVector.empty("v", allocator)) {
      validate(vector);
      setVector(vector, Arrays.asList(1, 2, 3, 4), Arrays.asList(5, 6));
      validate(vector);

      vector.getDataVector().setValueCount(4);
      ValidateUtil.ValidateException e =
          assertThrows(ValidateUtil.ValidateException.class, () -> validate(vector));
      assertTrue(e.getMessage().contains("Inner vector does not contain enough elements."));
    }
  }

  @Test
  public void testFixedSizeListVector() {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("v", 3, allocator)) {
      validate(vector);
      setVector(vector, Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6));
      validate(vector);

      vector.getDataVector().setValueCount(3);
      ValidateUtil.ValidateException e =
          assertThrows(ValidateUtil.ValidateException.class, () -> validate(vector));
      assertTrue(e.getMessage().contains("Inner vector does not contain enough elements."));
    }
  }

  @Test
  public void testStructVectorRangeEquals() {
    try (final StructVector vector = StructVector.empty("struct", allocator)) {
      vector.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);

      validate(vector);

      NullableStructWriter writer = vector.getWriter();
      writer.allocate();

      writeStructVector(writer, 1, 10L);
      writeStructVector(writer, 2, 20L);
      writeStructVector(writer, 3, 30L);
      writeStructVector(writer, 4, 40L);
      writeStructVector(writer, 5, 50L);
      writer.setValueCount(5);

      vector.getChild("f0").setValueCount(2);
      ValidateUtil.ValidateException e =
          assertThrows(ValidateUtil.ValidateException.class, () -> validate(vector));
      assertTrue(e.getMessage().contains("Struct vector length not equal to child vector length"));

      vector.getChild("f0").setValueCount(5);
      validate(vector);

      vector.getChild("f0").getDataBuffer().capacity(0);
      ValidateUtil.ValidateException e2 =
          assertThrows(ValidateUtil.ValidateException.class, () -> validate(vector));
      assertTrue(e2.getMessage().contains("Not enough capacity for fixed width data buffer"));
    }
  }

  @Test
  public void testUnionVector() {
    try (final UnionVector vector = UnionVector.empty("union", allocator)) {
      validate(vector);

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

      validate(vector);

      vector.getChildrenFromFields().get(0).setValueCount(1);
      ValidateUtil.ValidateException e1 =
          assertThrows(ValidateUtil.ValidateException.class, () -> validate(vector));
      assertTrue(e1.getMessage().contains("Union vector length not equal to child vector length"));

      vector.getChildrenFromFields().get(0).setValueCount(2);
      validate(vector);

      vector.getChildrenFromFields().get(0).getDataBuffer().capacity(0);
      ValidateUtil.ValidateException e2 =
          assertThrows(ValidateUtil.ValidateException.class, () -> validate(vector));
      assertTrue(e2.getMessage().contains("Not enough capacity for fixed width data buffer"));
    }
  }

  @Test
  public void testDenseUnionVector() {
    try (final DenseUnionVector vector = DenseUnionVector.empty("union", allocator)) {
      validate(vector);

      final NullableFloat4Holder float4Holder = new NullableFloat4Holder();
      float4Holder.value = 1.01f;
      float4Holder.isSet = 1;

      final NullableFloat8Holder float8Holder = new NullableFloat8Holder();
      float8Holder.value = 2.02f;
      float8Holder.isSet = 1;

      byte float4TypeId =
          vector.registerNewTypeId(Field.nullable("", Types.MinorType.FLOAT4.getType()));
      byte float8TypeId =
          vector.registerNewTypeId(Field.nullable("", Types.MinorType.FLOAT8.getType()));

      vector.setTypeId(0, float4TypeId);
      vector.setSafe(0, float4Holder);
      vector.setTypeId(1, float8TypeId);
      vector.setSafe(1, float8Holder);
      vector.setValueCount(2);

      validate(vector);

      vector.getChildrenFromFields().get(0).getDataBuffer().capacity(0);
      ValidateUtil.ValidateException e =
          assertThrows(ValidateUtil.ValidateException.class, () -> validate(vector));
      assertTrue(e.getMessage().contains("Not enough capacity for fixed width data buffer"));
    }
  }

  @Test
  public void testBaseFixedWidthVectorInstanceMethod() {
    try (final IntVector vector = new IntVector("v", allocator)) {
      vector.validate();
      setVector(vector, 1, 2, 3);
      vector.validate();

      vector.getDataBuffer().capacity(0);
      ValidateUtil.ValidateException e =
          assertThrows(ValidateUtil.ValidateException.class, () -> vector.validate());
      assertTrue(e.getMessage().contains("Not enough capacity for fixed width data buffer"));
    }
  }

  @Test
  public void testRunEndEncodedVector() {
    final FieldType valueType = FieldType.notNullable(Types.MinorType.BIGINT.getType());
    final FieldType runEndType = FieldType.notNullable(Types.MinorType.INT.getType());

    final Field valueField = new Field("value", valueType, null);
    final Field runEndField = new Field("ree", runEndType, null);

    try (RunEndEncodedVector vector =
        new RunEndEncodedVector(
            new Field(
                "ree",
                FieldType.notNullable(RunEndEncoded.INSTANCE),
                List.of(runEndField, valueField)),
            allocator,
            null)) {
      vector.validate();

      int runCount = 1;
      vector.allocateNew();
      ((BigIntVector) vector.getValuesVector()).set(0, 1);
      ((IntVector) vector.getRunEndsVector()).set(0, 10);
      vector.getValuesVector().setValueCount(runCount);
      vector.getRunEndsVector().setValueCount(runCount);
      vector.setValueCount(10);

      vector.validate();

      vector.getRunEndsVector().setValueCount(0);
      ValidateUtil.ValidateException e =
          assertThrows(ValidateUtil.ValidateException.class, () -> vector.validate());
      assertTrue(e.getMessage().contains("Run end vector does not contain enough elements"));
    }
  }

  private void writeStructVector(NullableStructWriter writer, int value1, long value2) {
    writer.start();
    writer.integer("f0").writeInt(value1);
    writer.bigInt("f1").writeBigInt(value2);
    writer.end();
  }
}

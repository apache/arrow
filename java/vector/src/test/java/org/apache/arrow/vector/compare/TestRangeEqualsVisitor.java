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

package org.apache.arrow.vector.compare;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRangeEqualsVisitor {

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
  public void testZeroVectorEquals() {
    try (final ZeroVector vector1 = new ZeroVector();
        final ZeroVector vector2 = new ZeroVector()) {

      VectorEqualsVisitor visitor = new VectorEqualsVisitor(vector2);
      assertTrue(vector1.accept(visitor));
    }
  }

  @Test
  public void testZeroVectorNotEquals() {
    try (final IntVector intVector = new IntVector("int", allocator);
        final ZeroVector zeroVector = new ZeroVector()) {

      VectorEqualsVisitor zeroVisitor = new VectorEqualsVisitor(zeroVector);
      assertFalse(intVector.accept(zeroVisitor));

      VectorEqualsVisitor intVisitor = new VectorEqualsVisitor(intVector);
      assertFalse(zeroVector.accept(intVisitor));
    }
  }

  @Test
  public void testIntVectorEqualsWithNull() {
    try (final IntVector vector1 = new IntVector("int", allocator);
        final IntVector vector2 = new IntVector("int", allocator)) {

      vector1.allocateNew(2);
      vector1.setValueCount(2);
      vector2.allocateNew(2);
      vector2.setValueCount(2);

      vector1.setSafe(0, 1);
      vector1.setSafe(1, 2);

      vector2.setSafe(0, 1);
      VectorEqualsVisitor visitor = new VectorEqualsVisitor(vector2);

      assertFalse(vector1.accept(visitor));
    }
  }

  @Test
  public void testIntVectorEquals() {
    try (final IntVector vector1 = new IntVector("int", allocator);
        final IntVector vector2 = new IntVector("int", allocator)) {

      vector1.allocateNew(3);
      vector1.setValueCount(3);
      vector2.allocateNew(3);
      vector2.setValueCount(2);

      vector1.setSafe(0, 1);
      vector1.setSafe(1, 2);
      vector1.setSafe(2, 3);

      vector2.setSafe(0, 1);
      vector2.setSafe(1, 2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor(vector2);

      assertFalse(vector1.accept(visitor));

      vector2.setValueCount(3);
      vector2.setSafe(2, 2);
      assertFalse(vector1.equals(vector2));

      vector2.setSafe(2, 3);
      assertTrue(vector1.accept(visitor));
    }
  }

  @Test
  public void testDecimalVectorEquals() {
    try (final DecimalVector vector1 = new DecimalVector("decimal", allocator, 3, 3);
        final DecimalVector vector2 = new DecimalVector("decimal", allocator, 3, 3);
        final DecimalVector vector3 = new DecimalVector("decimal", allocator, 3, 2)) {

      vector1.allocateNew(2);
      vector1.setValueCount(2);
      vector2.allocateNew(2);
      vector2.setValueCount(2);
      vector3.allocateNew(2);
      vector3.setValueCount(2);

      vector1.setSafe(0, 100);
      vector1.setSafe(1, 200);

      vector2.setSafe(0, 100);
      vector2.setSafe(1, 200);

      vector3.setSafe(0, 100);
      vector3.setSafe(1, 200);

      VectorEqualsVisitor visitor1 = new VectorEqualsVisitor(vector2);
      VectorEqualsVisitor visitor2 = new VectorEqualsVisitor(vector3);

      assertTrue(vector1.accept(visitor1));
      assertFalse(vector1.accept(visitor2));
    }
  }

  @Test
  public void testVarcharVectorEuqalsWithNull() {
    try (final VarCharVector vector1 = new VarCharVector("varchar", allocator);
        final VarCharVector vector2 = new VarCharVector("varchar", allocator)) {

      vector1.allocateNew();
      vector2.allocateNew();

      // set some values
      vector1.setSafe(0, STR1, 0, STR1.length);
      vector1.setSafe(1, STR2, 0, STR2.length);
      vector1.setValueCount(2);

      vector2.setSafe(0, STR1, 0, STR1.length);
      vector2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor(vector2);
      assertFalse(vector1.accept(visitor));
    }
  }

  @Test
  public void testVarcharVectorEquals() {
    try (final VarCharVector vector1 = new VarCharVector("varchar", allocator);
        final VarCharVector vector2 = new VarCharVector("varchar", allocator)) {

      vector1.allocateNew();
      vector2.allocateNew();

      // set some values
      vector1.setSafe(0, STR1, 0, STR1.length);
      vector1.setSafe(1, STR2, 0, STR2.length);
      vector1.setSafe(2, STR3, 0, STR3.length);
      vector1.setValueCount(3);

      vector2.setSafe(0, STR1, 0, STR1.length);
      vector2.setSafe(1, STR2, 0, STR2.length);
      vector2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor(vector2);
      assertFalse(vector1.accept(visitor));

      vector2.setSafe(2, STR3, 0, STR3.length);
      vector2.setValueCount(3);
      assertTrue(vector1.accept(visitor));
    }
  }

  @Test
  public void testVarBinaryVectorEquals() {
    try (final VarBinaryVector vector1 = new VarBinaryVector("binary", allocator);
        final VarBinaryVector vector2 = new VarBinaryVector("binary", allocator)) {

      vector1.allocateNew();
      vector2.allocateNew();

      // set some values
      vector1.setSafe(0, STR1, 0, STR1.length);
      vector1.setSafe(1, STR2, 0, STR2.length);
      vector1.setSafe(2, STR3, 0, STR3.length);
      vector1.setValueCount(3);

      vector2.setSafe(0, STR1, 0, STR1.length);
      vector2.setSafe(1, STR2, 0, STR2.length);
      vector2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor(vector2);
      assertFalse(vector1.accept(visitor));

      vector2.setSafe(2, STR3, 0, STR3.length);
      vector2.setValueCount(3);
      assertTrue(vector1.accept(visitor));
    }
  }

  @Test
  public void testListVectorEqualsWithNull() {
    try (final ListVector vector1 = ListVector.empty("list", allocator);
        final ListVector vector2 = ListVector.empty("list", allocator);) {

      UnionListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      //set some values
      writeListVector(writer1, new int[] {1, 2});
      writeListVector(writer1, new int[] {3, 4});
      writeListVector(writer1, new int[] {});
      writer1.setValueCount(3);

      UnionListWriter writer2 = vector2.getWriter();
      writer2.allocate();

      //set some values
      writeListVector(writer2, new int[] {1, 2});
      writeListVector(writer2, new int[] {3, 4});
      writer2.setValueCount(3);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor(vector2);

      assertFalse(vector1.accept(visitor));
    }
  }

  @Test
  public void testListVectorEquals() {
    try (final ListVector vector1 = ListVector.empty("list", allocator);
        final ListVector vector2 = ListVector.empty("list", allocator);) {

      UnionListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      //set some values
      writeListVector(writer1, new int[] {1, 2});
      writeListVector(writer1, new int[] {3, 4});
      writeListVector(writer1, new int[] {5, 6});
      writer1.setValueCount(3);

      UnionListWriter writer2 = vector2.getWriter();
      writer2.allocate();

      //set some values
      writeListVector(writer2, new int[] {1, 2});
      writeListVector(writer2, new int[] {3, 4});
      writer2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor(vector2);
      assertFalse(vector1.accept(visitor));

      writeListVector(writer2, new int[] {5, 6});
      writer2.setValueCount(3);

      assertTrue(vector1.accept(visitor));
    }
  }

  @Test
  public void testStructVectorEqualsWithNull() {

    try (final StructVector vector1 = StructVector.empty("struct", allocator);
        final StructVector vector2 = StructVector.empty("struct", allocator);) {
      vector1.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector1.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
      vector2.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector2.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);

      NullableStructWriter writer1 = vector1.getWriter();
      writer1.allocate();

      writeStructVector(writer1, 1, 10L);
      writeStructVector(writer1, 2, 20L);
      writeStructVector(writer1, 3, 30L);
      writer1.setValueCount(3);

      NullableStructWriter writer2 = vector2.getWriter();
      writer2.allocate();

      writeStructVector(writer2, 1, 10L);
      writeStructVector(writer2, 3, 30L);
      writer2.setValueCount(3);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor(vector2);
      assertFalse(vector1.accept(visitor));
    }
  }

  @Test
  public void testStructVectorEquals() {
    try (final StructVector vector1 = StructVector.empty("struct", allocator);
        final StructVector vector2 = StructVector.empty("struct", allocator);) {
      vector1.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector1.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
      vector2.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector2.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);

      NullableStructWriter writer1 = vector1.getWriter();
      writer1.allocate();

      writeStructVector(writer1, 1, 10L);
      writeStructVector(writer1, 2, 20L);
      writeStructVector(writer1, 3, 30L);
      writer1.setValueCount(3);

      NullableStructWriter writer2 = vector2.getWriter();
      writer2.allocate();

      writeStructVector(writer2, 1, 10L);
      writeStructVector(writer2, 2, 20L);
      writer2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor(vector2);
      assertFalse(vector1.accept(visitor));

      writeStructVector(writer2, 3, 30L);
      writer2.setValueCount(3);

      assertTrue(vector1.accept(visitor));
    }
  }

  @Test
  public void testStructVectorEqualsWithDiffChild() {
    try (final StructVector vector1 = StructVector.empty("struct", allocator);
        final StructVector vector2 = StructVector.empty("struct", allocator);) {
      vector1.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector1.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
      vector2.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector2.addOrGet("f10", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);

      NullableStructWriter writer1 = vector1.getWriter();
      writer1.allocate();

      writeStructVector(writer1, 1, 10L);
      writeStructVector(writer1, 2, 20L);
      writer1.setValueCount(2);

      NullableStructWriter writer2 = vector2.getWriter();
      writer2.allocate();

      writeStructVector(writer2, 1, 10L);
      writeStructVector(writer2, 2, 20L);
      writer2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor(vector2);
      assertFalse(vector1.accept(visitor));
    }
  }

  @Test
  public void testUnionVectorEquals() {
    try (final UnionVector vector1 = new UnionVector("union", allocator, null);
        final UnionVector vector2 = new UnionVector("union", allocator, null);) {

      final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
      uInt4Holder.value = 10;
      uInt4Holder.isSet = 1;

      final NullableIntHolder intHolder = new NullableIntHolder();
      uInt4Holder.value = 20;
      uInt4Holder.isSet = 1;

      vector1.setType(0, Types.MinorType.UINT4);
      vector1.setSafe(0, uInt4Holder);

      vector1.setType(1, Types.MinorType.INT);
      vector1.setSafe(1, intHolder);
      vector1.setValueCount(2);

      vector2.setType(0, Types.MinorType.UINT4);
      vector2.setSafe(0, uInt4Holder);

      vector2.setType(1, Types.MinorType.INT);
      vector2.setSafe(1, intHolder);
      vector2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor(vector2);
      assertTrue(vector1.accept(visitor));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEqualsWithIndexOutOfRange() {
    try (final IntVector vector1 = new IntVector("int", allocator);
        final IntVector vector2 = new IntVector("int", allocator)) {

      vector1.allocateNew(2);
      vector1.setValueCount(2);
      vector2.allocateNew(2);
      vector2.setValueCount(2);

      vector1.setSafe(0, 1);
      vector1.setSafe(1, 2);

      vector2.setSafe(0, 1);
      vector2.setSafe(1, 2);

      RangeEqualsVisitor visitor = new RangeEqualsVisitor(vector2, 3, 2, 1);
      assertTrue(vector1.accept(visitor));
    }
  }

  @Test
  public void testEqualsWithOutTypeCheck() {
    try (final IntVector intVector = new IntVector("int", allocator);
        final ZeroVector zeroVector = new ZeroVector()) {

      VectorEqualsVisitor zeroVisitor = new VectorEqualsVisitor(zeroVector);
      zeroVisitor.setTypeCheckNeeded(false);
      assertTrue(intVector.accept(zeroVisitor));

      VectorEqualsVisitor intVisitor = new VectorEqualsVisitor(intVector);
      intVisitor.setTypeCheckNeeded(false);
      assertTrue(zeroVector.accept(intVisitor));
    }
  }

  private void writeStructVector(NullableStructWriter writer, int value1, long value2) {
    writer.start();
    writer.integer("f0").writeInt(value1);
    writer.bigInt("f1").writeBigInt(value2);
    writer.end();
  }

  private void writeListVector(UnionListWriter writer, int[] values) {
    writer.startList();
    for (int v: values) {
      writer.integer().writeInt(v);
    }
    writer.endList();
  }
}

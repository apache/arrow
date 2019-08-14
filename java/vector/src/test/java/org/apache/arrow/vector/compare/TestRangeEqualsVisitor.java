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
import org.apache.arrow.vector.IntVector;
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
  public void testBaseFixedWidthVectorRangeEqual() {
    try (final IntVector vector1 = new IntVector("int", allocator);
        final IntVector vector2 = new IntVector("int", allocator)) {

      vector1.allocateNew(5);
      vector1.setValueCount(5);
      vector2.allocateNew(5);
      vector2.setValueCount(5);

      vector1.setSafe(0, 1);
      vector1.setSafe(1, 2);
      vector1.setSafe(2, 3);
      vector1.setSafe(3, 4);
      vector1.setSafe(4, 5);

      vector2.setSafe(0, 11);
      vector2.setSafe(1, 2);
      vector2.setSafe(2,3);
      vector2.setSafe(3,4);
      vector2.setSafe(4,55);

      RangeEqualsVisitor visitor = new RangeEqualsVisitor(vector2, 1, 1, 3);
      assertTrue(vector1.accept(visitor));
    }
  }

  @Test
  public void testBaseVariableVectorRangeEquals() {
    try (final VarCharVector vector1 = new VarCharVector("varchar", allocator);
        final VarCharVector vector2 = new VarCharVector("varchar", allocator)) {

      vector1.allocateNew();
      vector2.allocateNew();

      // set some values
      vector1.setSafe(0, STR1, 0, STR1.length);
      vector1.setSafe(1, STR2, 0, STR2.length);
      vector1.setSafe(2, STR3, 0, STR3.length);
      vector1.setSafe(3, STR2, 0, STR2.length);
      vector1.setSafe(4, STR1, 0, STR1.length);
      vector1.setValueCount(5);

      vector2.setSafe(0, STR1, 0, STR1.length);
      vector2.setSafe(1, STR2, 0, STR2.length);
      vector2.setSafe(2, STR3, 0, STR3.length);
      vector2.setSafe(3, STR2, 0, STR2.length);
      vector2.setSafe(4, STR1, 0, STR1.length);
      vector2.setValueCount(5);

      RangeEqualsVisitor visitor = new RangeEqualsVisitor(vector2, 1, 1, 3);
      assertTrue(vector1.accept(visitor));
    }
  }

  @Test
  public void testListVectorRangeEquals() {
    try (final ListVector vector1 = ListVector.empty("list", allocator);
        final ListVector vector2 = ListVector.empty("list", allocator);) {

      UnionListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      //set some values
      writeListVector(writer1, new int[] {1, 2});
      writeListVector(writer1, new int[] {3, 4});
      writeListVector(writer1, new int[] {5, 6});
      writeListVector(writer1, new int[] {7, 8});
      writeListVector(writer1, new int[] {9, 10});
      writer1.setValueCount(5);

      UnionListWriter writer2 = vector2.getWriter();
      writer2.allocate();

      //set some values
      writeListVector(writer2, new int[] {0, 0});
      writeListVector(writer2, new int[] {3, 4});
      writeListVector(writer2, new int[] {5, 6});
      writeListVector(writer2, new int[] {7, 8});
      writeListVector(writer2, new int[] {0, 0});
      writer2.setValueCount(5);

      RangeEqualsVisitor visitor = new RangeEqualsVisitor(vector2, 1, 1, 3);
      assertTrue(vector1.accept(visitor));
    }
  }

  @Test
  public void testStructVectorRangeEquals() {
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
      writeStructVector(writer1, 4, 40L);
      writeStructVector(writer1, 5, 50L);
      writer1.setValueCount(5);

      NullableStructWriter writer2 = vector2.getWriter();
      writer2.allocate();

      writeStructVector(writer2, 0, 00L);
      writeStructVector(writer2, 2, 20L);
      writeStructVector(writer2, 3, 30L);
      writeStructVector(writer2, 4, 40L);
      writeStructVector(writer2, 0, 0L);
      writer2.setValueCount(5);

      RangeEqualsVisitor visitor = new RangeEqualsVisitor(vector2, 1, 1, 3);
      assertTrue(vector1.accept(visitor));
    }
  }

  @Test
  public void testUnionVectorRangeEquals() {
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

      vector1.setType(2, Types.MinorType.INT);
      vector1.setSafe(2, intHolder);
      vector1.setValueCount(3);

      vector2.setType(0, Types.MinorType.UINT4);
      vector2.setSafe(0, uInt4Holder);

      vector2.setType(1, Types.MinorType.INT);
      vector2.setSafe(1, intHolder);

      vector2.setType(2, Types.MinorType.INT);
      vector2.setSafe(2, intHolder);
      vector2.setValueCount(3);

      RangeEqualsVisitor visitor = new RangeEqualsVisitor(vector2, 1, 1, 2);
      assertTrue(vector1.accept(visitor));
    }
  }

  @Test
  public void testEqualsWithOutTypeCheck() {
    try (final IntVector intVector = new IntVector("int", allocator);
        final ZeroVector zeroVector = new ZeroVector()) {

      VectorEqualsVisitor zeroVisitor = new VectorEqualsVisitor(zeroVector, false);
      assertTrue(intVector.accept(zeroVisitor));

      VectorEqualsVisitor intVisitor = new VectorEqualsVisitor(intVector, false);
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

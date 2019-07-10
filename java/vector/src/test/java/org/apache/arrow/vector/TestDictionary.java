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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDictionary {

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
  public void testEquals() {

    //test Int
    try (final IntVector vector1 = new IntVector("v1", allocator);
         final IntVector vector2 = new IntVector("v2", allocator)) {

      Dictionary dict1 = new Dictionary(vector1, new DictionaryEncoding(1L, false, null));
      Dictionary dict2 = new Dictionary(vector2, new DictionaryEncoding(1L, false, null));

      vector1.allocateNew(3);
      vector1.setValueCount(3);
      vector2.allocateNew(3);
      vector2.setValueCount(3);

      vector1.setSafe(0, 1);
      vector1.setSafe(1, 2);
      vector1.setSafe(2, 3);

      vector2.setSafe(0, 1);
      vector2.setSafe(1, 2);
      vector2.setSafe(2, 0);


      assertFalse(dict1.equals(dict2));

      vector2.setSafe(2, 3);
      assertTrue(dict1.equals(dict2));

    }

    //test List
    try (final ListVector vector1 = ListVector.empty("v1", allocator);
        final ListVector vector2 = ListVector.empty("v2", allocator);) {

      Dictionary dict1 = new Dictionary(vector1, new DictionaryEncoding(1L, false, null));
      Dictionary dict2 = new Dictionary(vector2, new DictionaryEncoding(1L, false, null));

      UnionListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      //set some values
      writeListVector(writer1, new int[]{1, 2});
      writeListVector(writer1, new int[]{3, 4});
      writeListVector(writer1, new int[]{5, 6});
      writer1.setValueCount(3);

      UnionListWriter writer2 = vector2.getWriter();
      writer2.allocate();

      //set some values
      writeListVector(writer2, new int[]{1, 2});
      writeListVector(writer2, new int[]{3, 4});
      writeListVector(writer2, new int[]{5, 6});
      writer2.setValueCount(3);

      assertTrue(dict1.equals(dict2));

    }

    //test Struct
    try (final StructVector vector1 = StructVector.empty("v1", allocator);
         final StructVector vector2 = StructVector.empty("v2", allocator);) {
      vector1.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector1.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
      vector2.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector2.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);

      Dictionary dict1 = new Dictionary(vector1, new DictionaryEncoding(1L, false, null));
      Dictionary dict2 = new Dictionary(vector2, new DictionaryEncoding(1L, false, null));

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

      assertTrue(dict1.equals(dict2));

    }

    //test Union
    try (final UnionVector vector1 = new UnionVector("v1", allocator, null);
         final UnionVector vector2 = new UnionVector("v1", allocator, null);) {

      final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
      uInt4Holder.value = 10;
      uInt4Holder.isSet = 1;

      final NullableIntHolder intHolder = new NullableIntHolder();
      uInt4Holder.value = 20;
      uInt4Holder.isSet = 1;

      vector1.setType(0, Types.MinorType.UINT4);
      vector1.setSafe(0, uInt4Holder);

      vector1.setType(2, Types.MinorType.INT);
      vector1.setSafe(2, intHolder);
      vector1.setValueCount(3);

      vector2.setType(0, Types.MinorType.UINT4);
      vector2.setSafe(0, uInt4Holder);

      vector2.setType(2, Types.MinorType.INT);
      vector2.setSafe(2, intHolder);
      vector2.setValueCount(3);

      Dictionary dict1 = new Dictionary(vector1, new DictionaryEncoding(1L, false, null));
      Dictionary dict2 = new Dictionary(vector2, new DictionaryEncoding(1L, false, null));

      assertTrue(dict1.equals(dict2));

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

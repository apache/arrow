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

import com.google.common.collect.Lists;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListReader;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFixedSizeListVector {

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
  public void testIntType() {
    try (FixedSizeListVector vector = FixedSizeListVector.empty("list", 2, allocator)) {
      IntVector nested = (IntVector) vector.addOrGetVector(FieldType.nullable(MinorType.INT.getType())).getVector();
      vector.allocateNew();

      for (int i = 0; i < 10; i++) {
        vector.setNotNull(i);
        nested.set(i * 2, i);
        nested.set(i * 2 + 1, i + 10);
      }
      vector.setValueCount(10);

      UnionFixedSizeListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        Assert.assertTrue(reader.isSet());
        Assert.assertTrue(reader.next());
        Assert.assertEquals(i, reader.reader().readInteger().intValue());
        Assert.assertTrue(reader.next());
        Assert.assertEquals(i + 10, reader.reader().readInteger().intValue());
        Assert.assertFalse(reader.next());
        Assert.assertEquals(Lists.newArrayList(i, i + 10), reader.readObject());
      }
    }
  }

  @Test
  public void testFloatTypeNullable() {
    try (FixedSizeListVector vector = FixedSizeListVector.empty("list", 2, allocator)) {
      Float4Vector nested = (Float4Vector) vector.addOrGetVector(FieldType.nullable(MinorType.FLOAT4.getType())).getVector();
      vector.allocateNew();

      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector.setNotNull(i);
          nested.set(i * 2, i + 0.1f);
          nested.set(i * 2 + 1, i + 10.1f);
        }
      }
      vector.setValueCount(10);

      UnionFixedSizeListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        if (i % 2 == 0) {
          Assert.assertTrue(reader.isSet());
          Assert.assertTrue(reader.next());
          Assert.assertEquals(i + 0.1f, reader.reader().readFloat(), 0.00001);
          Assert.assertTrue(reader.next());
          Assert.assertEquals(i + 10.1f, reader.reader().readFloat(), 0.00001);
          Assert.assertFalse(reader.next());
          Assert.assertEquals(Lists.newArrayList(i + 0.1f, i + 10.1f), reader.readObject());
        } else {
          Assert.assertFalse(reader.isSet());
          Assert.assertNull(reader.readObject());
        }
      }
    }
  }

  @Test
  public void testNestedInList() {
    try (ListVector vector = ListVector.empty("list", allocator)) {
      FixedSizeListVector tuples = (FixedSizeListVector) vector.addOrGetVector(FieldType.nullable(new ArrowType.FixedSizeList(2))).getVector();
      IntVector innerVector = (IntVector) tuples.addOrGetVector(FieldType.nullable(MinorType.INT.getType())).getVector();
      vector.allocateNew();

      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          int position = vector.startNewValue(i);
          for (int j = 0; j < i % 7; j++) {
            tuples.setNotNull(position + j);
            innerVector.set((position + j) * 2, j);
            innerVector.set((position + j) * 2 + 1, j + 1);
          }
          vector.endValue(i, i % 7);
        }
      }
      vector.setValueCount(10);

      UnionListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        if (i % 2 == 0) {
          for (int j = 0; j < i % 7; j++) {
            Assert.assertTrue(reader.next());
            FieldReader innerListReader = reader.reader();
            for (int k = 0; k < 2; k++) {
              Assert.assertTrue(innerListReader.next());
              Assert.assertEquals(k + j, innerListReader.reader().readInteger().intValue());
            }
            Assert.assertFalse(innerListReader.next());
          }
          Assert.assertFalse(reader.next());
        } else {
          Assert.assertFalse(reader.isSet());
          Assert.assertNull(reader.readObject());
        }
      }
    }
  }

  @Test
  public void testTransferPair() {
    try (FixedSizeListVector from = new FixedSizeListVector("from", allocator, 2, null, null);
         FixedSizeListVector to = new FixedSizeListVector("to", allocator, 2, null, null)) {
      Float4Vector nested = (Float4Vector) from.addOrGetVector(FieldType.nullable(MinorType.FLOAT4.getType())).getVector();
      from.allocateNew();

      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          from.setNotNull(i);
          nested.set(i * 2, i + 0.1f);
          nested.set(i * 2 + 1, i + 10.1f);
        }
      }
      from.setValueCount(10);

      TransferPair pair = from.makeTransferPair(to);

      pair.copyValueSafe(0, 1);
      pair.copyValueSafe(2, 2);
      to.copyFromSafe(4, 3, from);

      to.setValueCount(10);

      UnionFixedSizeListReader reader = to.getReader();

      reader.setPosition(0);
      Assert.assertFalse(reader.isSet());
      Assert.assertNull(reader.readObject());

      reader.setPosition(1);
      Assert.assertTrue(reader.isSet());
      Assert.assertTrue(reader.next());
      Assert.assertEquals(0.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertTrue(reader.next());
      Assert.assertEquals(10.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertFalse(reader.next());
      Assert.assertEquals(Lists.newArrayList(0.1f, 10.1f), reader.readObject());

      reader.setPosition(2);
      Assert.assertTrue(reader.isSet());
      Assert.assertTrue(reader.next());
      Assert.assertEquals(2.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertTrue(reader.next());
      Assert.assertEquals(12.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertFalse(reader.next());
      Assert.assertEquals(Lists.newArrayList(2.1f, 12.1f), reader.readObject());

      reader.setPosition(3);
      Assert.assertTrue(reader.isSet());
      Assert.assertTrue(reader.next());
      Assert.assertEquals(4.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertTrue(reader.next());
      Assert.assertEquals(14.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertFalse(reader.next());
      Assert.assertEquals(Lists.newArrayList(4.1f, 14.1f), reader.readObject());

      for (int i = 4; i < 10; i++) {
        reader.setPosition(i);
        Assert.assertFalse(reader.isSet());
        Assert.assertNull(reader.readObject());
      }
    }
  }

  @Test
  public void testConsistentChildName() throws Exception {
    try (FixedSizeListVector listVector = FixedSizeListVector.empty("sourceVector", 2, allocator)) {
      String emptyListStr = listVector.getField().toString();
      Assert.assertTrue(emptyListStr.contains(ListVector.DATA_VECTOR_NAME));

      listVector.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()));
      String emptyVectorStr = listVector.getField().toString();
      Assert.assertTrue(emptyVectorStr.contains(ListVector.DATA_VECTOR_NAME));
    }
  }

}

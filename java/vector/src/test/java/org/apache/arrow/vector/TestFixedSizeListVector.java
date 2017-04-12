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
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
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
    try (FixedSizeListVector vector = new FixedSizeListVector("list", allocator, 2, null, null)) {
      NullableIntVector nested = (NullableIntVector) vector.addOrGetVector(FieldType.nullable(MinorType.INT.getType())).getVector();
      NullableIntVector.Mutator mutator = nested.getMutator();
      vector.allocateNew();

      for (int i = 0; i < 10; i++) {
        vector.getMutator().setNotNull(i);
        mutator.set(i * 2, i);
        mutator.set(i * 2 + 1, i + 10);
      }
      vector.getMutator().setValueCount(10);

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
    try (FixedSizeListVector vector = new FixedSizeListVector("list", allocator, 2, null, null)) {
      NullableFloat4Vector nested = (NullableFloat4Vector) vector.addOrGetVector(FieldType.nullable(MinorType.FLOAT4.getType())).getVector();
      NullableFloat4Vector.Mutator mutator = nested.getMutator();
      vector.allocateNew();

      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector.getMutator().setNotNull(i);
          mutator.set(i * 2, i + 0.1f);
          mutator.set(i * 2 + 1, i + 10.1f);
        }
      }
      vector.getMutator().setValueCount(10);

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
    try (ListVector vector = new ListVector("list", allocator, null, null)) {
      ListVector.Mutator mutator = vector.getMutator();
      FixedSizeListVector tuples = (FixedSizeListVector) vector.addOrGetVector(FieldType.nullable(new ArrowType.FixedSizeList(2))).getVector();
      FixedSizeListVector.Mutator tupleMutator = tuples.getMutator();
      NullableIntVector.Mutator innerMutator = (NullableIntVector.Mutator) tuples.addOrGetVector(FieldType.nullable(MinorType.INT.getType())).getVector().getMutator();
      vector.allocateNew();

      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          int position = mutator.startNewValue(i);
          for (int j = 0; j < i % 7; j++) {
            tupleMutator.setNotNull(position + j);
            innerMutator.set((position + j) * 2, j);
            innerMutator.set((position + j) * 2 + 1, j + 1);
          }
          mutator.endValue(i, i % 7);
        }
      }
      mutator.setValueCount(10);

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
}

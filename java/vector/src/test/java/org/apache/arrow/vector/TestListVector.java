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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.*;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestListVector {

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
  public void testCopyFrom() throws Exception {
    try (ListVector inVector = ListVector.empty("input", allocator);
         ListVector outVector = ListVector.empty("output", allocator)) {
      UnionListWriter writer = inVector.getWriter();
      writer.allocate();

      // populate input vector with the following records
      // [1, 2, 3]
      // null
      // []
      writer.setPosition(0); // optional
      writer.startList();
      writer.bigInt().writeBigInt(1);
      writer.bigInt().writeBigInt(2);
      writer.bigInt().writeBigInt(3);
      writer.endList();

      writer.setPosition(2);
      writer.startList();
      writer.endList();

      writer.setValueCount(3);

      // copy values from input to output
      outVector.allocateNew();
      for (int i = 0; i < 3; i++) {
        outVector.copyFrom(i, i, inVector);
      }
      outVector.getMutator().setValueCount(3);

      // assert the output vector is correct
      FieldReader reader = outVector.getReader();
      Assert.assertTrue("shouldn't be null", reader.isSet());
      reader.setPosition(1);
      Assert.assertFalse("should be null", reader.isSet());
      reader.setPosition(2);
      Assert.assertTrue("shouldn't be null", reader.isSet());
    }
  }

  @Test
  public void testSetLastSetUsage() throws Exception {
    try (ListVector listVector = ListVector.empty("input", allocator)) {

      /* Explicitly add the dataVector */
      MinorType type = MinorType.BIGINT;
      listVector.addOrGetVector(FieldType.nullable(type.getType()));

      /* allocate memory */
      listVector.allocateNew();

      /* get inner vectors; bitVector and offsetVector */
      List<BufferBacked> innerVectors = listVector.getFieldInnerVectors();
      BitVector bitVector = (BitVector)innerVectors.get(0);
      UInt4Vector offsetVector = (UInt4Vector)innerVectors.get(1);

      /* get the underlying data vector -- NullableBigIntVector */
      NullableBigIntVector dataVector = (NullableBigIntVector)listVector.getDataVector();

      /* check current lastSet */
      assertEquals(Integer.toString(0), Integer.toString(listVector.getMutator().getLastSet()));

      int index = 0;
      int offset = 0;

      /* write [10, 11, 12] to the list vector at index */
      bitVector.getMutator().setSafe(index, 1);
      dataVector.getMutator().setSafe(0, 1, 10);
      dataVector.getMutator().setSafe(1, 1, 11);
      dataVector.getMutator().setSafe(2, 1, 12);
      offsetVector.getMutator().setSafe(index + 1, 3);

      index  += 1;

      /* write [13, 14] to the list vector at index 1 */
      bitVector.getMutator().setSafe(index, 1);
      dataVector.getMutator().setSafe(3, 1, 13);
      dataVector.getMutator().setSafe(4, 1, 14);
      offsetVector.getMutator().setSafe(index + 1, 5);

      index += 1;

      /* write [15, 16, 17] to the list vector at index 2 */
      bitVector.getMutator().setSafe(index, 1);
      dataVector.getMutator().setSafe(5, 1, 15);
      dataVector.getMutator().setSafe(6, 1, 16);
      dataVector.getMutator().setSafe(7, 1, 17);
      offsetVector.getMutator().setSafe(index + 1, 8);

      /* check current lastSet */
      assertEquals(Integer.toString(0), Integer.toString(listVector.getMutator().getLastSet()));

      /* set lastset and arbitrary valuecount for list vector.
       *
       * NOTE: if we don't execute setLastSet() before setLastValueCount(), then
       * the latter will corrupt the offsetVector and thus the accessor will not
       * retrieve the correct values from underlying dataVector. Run the test
       * by commenting out next line and we should see failures from 5th assert
       * onwards. This is why doing setLastSet() is important before setValueCount()
       * once the vector has been loaded.
       *
       * Another important thing to remember is the value of lastSet itself.
       * Even though the listVector has elements till index 2 only, the lastSet should
       * be set as 3. This is because the offsetVector has valid offsets filled till index 3.
       * If we do setLastSet(2), the offsetVector at index 3 will contain incorrect value
       * after execution of setValueCount().
       *
       * correct state of the listVector
       * bitvector    {1, 1, 1, 0, 0.... }
       * offsetvector {0, 3, 5, 8, 8, 8.....}
       * datavector   { [10, 11, 12],
       *                [13, 14],
       *                [15, 16, 17]
       *              }
       *
       * if we don't do setLastSet() before setValueCount --> incorrect state
       * bitvector    {1, 1, 1, 0, 0.... }
       * offsetvector {0, 0, 0, 0, 0, 0.....}
       * datavector   { [10, 11, 12],
       *                [13, 14],
       *                [15, 16, 17]
       *              }
       *
       * if we do setLastSet(2) before setValueCount --> incorrect state
       * bitvector    {1, 1, 1, 0, 0.... }
       * offsetvector {0, 3, 5, 5, 5, 5.....}
       * datavector   { [10, 11, 12],
       *                [13, 14],
       *                [15, 16, 17]
       *              }
       */
      listVector.getMutator().setLastSet(3);
      listVector.getMutator().setValueCount(10);

      /* check the vector output */
      final UInt4Vector.Accessor offsetAccessor = offsetVector.getAccessor();
      final ValueVector.Accessor valueAccessor = dataVector.getAccessor();

      index  = 0;
      offset = offsetAccessor.get(index);
      assertEquals(Integer.toString(0), Integer.toString(offset));

      Object actual = valueAccessor.getObject(offset);
      assertEquals(new Long(10), (Long)actual);
      offset++;
      actual = valueAccessor.getObject(offset);
      assertEquals(new Long(11), (Long)actual);
      offset++;
      actual = valueAccessor.getObject(offset);
      assertEquals(new Long(12), (Long)actual);

      index++;
      offset = offsetAccessor.get(index);
      assertEquals(Integer.toString(3), Integer.toString(offset));

      actual = valueAccessor.getObject(offset);
      assertEquals(new Long(13), (Long)actual);
      offset++;
      actual = valueAccessor.getObject(offset);
      assertEquals(new Long(14), (Long)actual);

      index++;
      offset = offsetAccessor.get(index);
      assertEquals(Integer.toString(5), Integer.toString(offset));

      actual = valueAccessor.getObject(offsetAccessor.get(index));
      assertEquals(new Long(15), (Long)actual);
      offset++;
      actual = valueAccessor.getObject(offset);
      assertEquals(new Long(16), (Long)actual);
      offset++;
      actual = valueAccessor.getObject(offset);
      assertEquals(new Long(17), (Long)actual);

      index++;
      offset = offsetAccessor.get(index);
      assertEquals(Integer.toString(8), Integer.toString(offset));

      actual = valueAccessor.getObject(offsetAccessor.get(index));
      assertNull(actual);
    }
  }
}

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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    try (ListVector inVector = new ListVector("input", allocator, null, null);
         ListVector outVector = new ListVector("output", allocator, null, null)) {
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
}

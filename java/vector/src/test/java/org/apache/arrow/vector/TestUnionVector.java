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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.holders.UInt4Holder;
import org.apache.arrow.vector.types.MaterializedField;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUnionVector {
  private final static String EMPTY_SCHEMA_PATH = "";

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
  public void testUnionVector() throws Exception {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, UInt4Holder.TYPE);

    final BufferAllocator alloc = new DirtyBufferAllocator(allocator, (byte) 100);

    UnionVector unionVector = new UnionVector(field, alloc, null);

    final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
    uInt4Holder.value = 100;
    uInt4Holder.isSet = 1;

    try {
      // write some data
      final UnionVector.Mutator mutator = unionVector.getMutator();
      mutator.setType(0, Types.MinorType.UINT4);
      mutator.setSafe(0, uInt4Holder);
      mutator.setType(2, Types.MinorType.UINT4);
      mutator.setSafe(2, uInt4Holder);
      mutator.setValueCount(4);

      // check that what we wrote is correct
      final UnionVector.Accessor accessor = unionVector.getAccessor();
      assertEquals(4, accessor.getValueCount());

      assertEquals(false, accessor.isNull(0));
      assertEquals(100, accessor.getObject(0));

      assertEquals(true, accessor.isNull(1));

      assertEquals(false, accessor.isNull(2));
      assertEquals(100, accessor.getObject(2));

      assertEquals(true, accessor.isNull(3));

    } finally {
      unionVector.clear();
    }
  }

}

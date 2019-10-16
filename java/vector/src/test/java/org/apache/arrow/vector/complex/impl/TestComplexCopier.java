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

package org.apache.arrow.vector.complex.impl;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestComplexCopier {

  private BufferAllocator allocator;

  private static final int COUNT = 100;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testCopyFixedSizeListVector() {
    try (FixedSizeListVector from = FixedSizeListVector.empty("from", 3, allocator);
         FixedSizeListVector to = FixedSizeListVector.empty("to", 3, allocator)) {

      from.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()));
      to.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()));

      // populate from vector
      UnionFixedSizeListWriter writer = from.getWriter();
      for (int i = 0; i < COUNT; i++) {
        writer.startList();
        writer.integer().writeInt(i);
        writer.integer().writeInt(i * 2);
        writer.integer().writeInt(i * 3);
        writer.endList();
      }
      from.setValueCount(COUNT);
      to.setValueCount(COUNT);

      // copy values
      FieldReader in = from.getReader();
      FieldWriter out = to.getWriter();
      for (int i = 0; i < COUNT; i++) {
        in.setPosition(i);
        out.setPosition(i);
        ComplexCopier.copy(in, out);
      }

      // validate equals
      assertTrue(VectorEqualsVisitor.vectorEquals(from, to));

    }
  }

  @Test
  public void testInvalidCopyFixedSizeListVector() {
    try (FixedSizeListVector from = FixedSizeListVector.empty("from", 3, allocator);
        FixedSizeListVector to = FixedSizeListVector.empty("to", 2, allocator)) {

      from.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()));
      to.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()));

      // populate from vector
      UnionFixedSizeListWriter writer = from.getWriter();
      for (int i = 0; i < COUNT; i++) {
        writer.startList();
        writer.integer().writeInt(i);
        writer.integer().writeInt(i * 2);
        writer.integer().writeInt(i * 3);
        writer.endList();
      }
      from.setValueCount(COUNT);
      to.setValueCount(COUNT);

      // copy values
      FieldReader in = from.getReader();
      FieldWriter out = to.getWriter();
      IllegalStateException e = assertThrows(IllegalStateException.class,
          () -> ComplexCopier.copy(in, out));
      assertTrue(e.getMessage().contains("greater than listSize"));
    }
  }
}

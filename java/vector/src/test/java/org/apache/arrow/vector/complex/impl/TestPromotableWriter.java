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

package org.apache.arrow.vector.complex.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DirtyRootAllocator;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPromotableWriter {
  private final static String EMPTY_SCHEMA_PATH = "";

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
  public void testPromoteToUnion() throws Exception {

    try (final MapVector container = MapVector.empty(EMPTY_SCHEMA_PATH, allocator);
         final MapVector v = container.addOrGetMap("test");
         final PromotableWriter writer = new PromotableWriter(v, container)) {

      container.allocateNew();

      writer.start();

      writer.setPosition(0);
      writer.bit("A").writeBit(0);

      writer.setPosition(1);
      writer.bit("A").writeBit(1);

      writer.decimal("dec", 10, 10);

      writer.setPosition(2);
      writer.integer("A").writeInt(10);

      // we don't write anything in 3

      writer.setPosition(4);
      writer.integer("A").writeInt(100);

      writer.end();

      container.setValueCount(5);

      final UnionVector uv = v.getChild("A", UnionVector.class);

      assertFalse("0 shouldn't be null", uv.isNull(0));
      assertEquals(false, uv.getObject(0));

      assertFalse("1 shouldn't be null", uv.isNull(1));
      assertEquals(true, uv.getObject(1));

      assertFalse("2 shouldn't be null", uv.isNull(2));
      assertEquals(10, uv.getObject(2));

      assertTrue("3 should be null", uv.isNull(3));

      assertFalse("4 shouldn't be null", uv.isNull(4));
      assertEquals(100, uv.getObject(4));

      container.clear();
      container.allocateNew();

      ComplexWriterImpl newWriter = new ComplexWriterImpl(EMPTY_SCHEMA_PATH, container);

      MapWriter newMapWriter = newWriter.rootAsMap();

      newMapWriter.start();

      newMapWriter.setPosition(2);
      newMapWriter.integer("A").writeInt(10);

      Field childField1 = container.getField().getChildren().get(0).getChildren().get(0);
      Field childField2 = container.getField().getChildren().get(0).getChildren().get(1);
      assertEquals("Child field should be union type: " + childField1.getName(), ArrowTypeID.Union, childField1.getType().getTypeID());
      assertEquals("Child field should be decimal type: " + childField2.getName(), ArrowTypeID.Decimal, childField2.getType().getTypeID());
    }
  }
}

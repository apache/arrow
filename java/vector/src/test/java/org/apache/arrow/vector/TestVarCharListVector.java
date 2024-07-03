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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestVarCharListVector {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testVarCharListWithNulls() {
    byte[] bytes = "a".getBytes(StandardCharsets.UTF_8);
    try (ListVector vector =
            new ListVector(
                "VarList", allocator, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        ArrowBuf tempBuf = allocator.buffer(bytes.length)) {
      UnionListWriter writer = vector.getWriter();
      writer.allocate();

      // populate input vector with the following records
      // ["a"]
      // null
      // ["b"]
      writer.setPosition(0); // optional
      writer.startList();
      tempBuf.setBytes(0, bytes);
      writer.writeVarChar(0, bytes.length, tempBuf);
      writer.endList();

      writer.setPosition(2);
      writer.startList();
      bytes = "b".getBytes(StandardCharsets.UTF_8);
      tempBuf.setBytes(0, bytes);
      writer.writeVarChar(0, bytes.length, tempBuf);
      writer.endList();

      writer.setValueCount(2);

      assertEquals(2, vector.getValueCount());
      assertEquals(2, vector.getDataVector().getValueCount());
    }
  }
}

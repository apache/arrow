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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.FieldType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestVectorReset {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  private void resetVectorAndVerify(ValueVector vector, ArrowBuf[] bufs) {
    int[] sizeBefore = new int[bufs.length];
    for (int i = 0; i < bufs.length; i++) {
      sizeBefore[i] = bufs[i].capacity();
    }
    vector.reset();
    for (int i = 0; i < bufs.length; i++) {
      assertEquals(sizeBefore[i], bufs[i].capacity());
      verifyBufferZeroed(bufs[i]);
    }
    assertEquals(0, vector.getValueCount());
  }

  private void verifyBufferZeroed(ArrowBuf buf) {
    for (int i = 0; i < buf.capacity(); i++) {
      assertTrue((byte) 0 == buf.getByte(i));
    }
  }

  @Test
  public void testFixedTypeReset() {
    try (final UInt4Vector vector = new UInt4Vector("UInt4", allocator)) {
      vector.allocateNewSafe();
      vector.setNull(0);
      vector.setValueCount(1);
      resetVectorAndVerify(vector, vector.getBuffers(false));
    }
  }

  @Test
  public void testVariableTypeReset() {
    try (final VarCharVector vector = new VarCharVector("VarChar", allocator)) {
      vector.allocateNewSafe();
      vector.set(0, "a".getBytes(StandardCharsets.UTF_8));
      vector.setLastSet(0);
      vector.setValueCount(1);
      resetVectorAndVerify(vector, vector.getBuffers(false));
      assertEquals(-1, vector.getLastSet());
    }
  }

  @Test
  public void testListTypeReset() {
    try (final ListVector variableList =
           new ListVector("VarList", allocator, FieldType.nullable(MinorType.INT.getType()), null);
         final FixedSizeListVector fixedList =
           new FixedSizeListVector("FixedList", allocator, FieldType.nullable(new FixedSizeList(2)), null)
    ) {
      // ListVector
      variableList.allocateNewSafe();
      variableList.startNewValue(0);
      variableList.endValue(0, 0);
      variableList.setValueCount(1);
      resetVectorAndVerify(variableList, variableList.getBuffers(false));
      assertEquals(0, variableList.getLastSet());

      // FixedSizeListVector
      fixedList.allocateNewSafe();
      fixedList.setNull(0);
      fixedList.setValueCount(1);
      resetVectorAndVerify(fixedList, fixedList.getBuffers(false));
    }
  }

  @Test
  public void testStructTypeReset() {
    try (final NonNullableStructVector nonNullableStructVector =
           new NonNullableStructVector("Struct", allocator, FieldType.nullable(MinorType.INT.getType()), null);
         final StructVector structVector =
           new StructVector("NullableStruct", allocator, FieldType.nullable(MinorType.INT.getType()), null)
    ) {
      // NonNullableStructVector
      nonNullableStructVector.allocateNewSafe();
      IntVector structChild = nonNullableStructVector
          .addOrGet("child", FieldType.nullable(new Int(32, true)), IntVector.class);
      structChild.setNull(0);
      nonNullableStructVector.setValueCount(1);
      resetVectorAndVerify(nonNullableStructVector, nonNullableStructVector.getBuffers(false));

      // StructVector
      structVector.allocateNewSafe();
      structVector.setNull(0);
      structVector.setValueCount(1);
      resetVectorAndVerify(structVector, structVector.getBuffers(false));
    }
  }

  @Test
  public void testUnionTypeReset() {
    try (final UnionVector vector = new UnionVector("Union", allocator, null);
         final IntVector dataVector = new IntVector("Int", allocator)
    ) {
      vector.getBufferSize();
      vector.allocateNewSafe();
      dataVector.allocateNewSafe();
      vector.addVector(dataVector);
      dataVector.setNull(0);
      vector.setValueCount(1);
      resetVectorAndVerify(vector, vector.getBuffers(false));
    }
  }
}

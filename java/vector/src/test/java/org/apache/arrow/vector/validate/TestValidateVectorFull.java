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

package org.apache.arrow.vector.validate;

import static org.apache.arrow.vector.testing.ValueVectorDataPopulator.setVector;
import static org.apache.arrow.vector.util.ValueVectorUtility.validateFull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestValidateVectorFull {

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
  public void testBaseVariableWidthVector() {
    try (final VarCharVector vector = new VarCharVector("v", allocator)) {
      validateFull(vector);
      setVector(vector, "aaa", "bbb", "ccc");
      validateFull(vector);

      ArrowBuf offsetBuf = vector.getOffsetBuffer();
      offsetBuf.setInt(0, 100);
      offsetBuf.setInt(4, 50);

      ValidateUtility.ValidateException e = assertThrows(ValidateUtility.ValidateException.class,
          () -> validateFull(vector));
      assertTrue(e.getMessage().contains("The values in the offset buffer are decreasing"));
    }
  }

  @Test
  public void testBaseLargeVariableWidthVector() {
    try (final LargeVarCharVector vector = new LargeVarCharVector("v", allocator)) {
      validateFull(vector);
      setVector(vector, "aaa", "bbb", null, "ccc");
      validateFull(vector);

      ArrowBuf offsetBuf = vector.getOffsetBuffer();
      offsetBuf.setInt(0, 100);
      offsetBuf.setInt(4, 50);

      ValidateUtility.ValidateException e = assertThrows(ValidateUtility.ValidateException.class,
          () -> validateFull(vector));
      assertTrue(e.getMessage().contains("The values in the large offset buffer are decreasing"));
    }
  }

  @Test
  public void testListVector() {
    try (final ListVector vector = ListVector.empty("v", allocator)) {
      validateFull(vector);
      setVector(vector, Arrays.asList(1, 2, 3), Arrays.asList(4, 5), Arrays.asList(6, 7, 8, 9));
      validateFull(vector);

      ArrowBuf offsetBuf = vector.getOffsetBuffer();
      offsetBuf.setInt(0, 100);
      offsetBuf.setInt(8, 50);

      ValidateUtility.ValidateException e = assertThrows(ValidateUtility.ValidateException.class,
          () -> validateFull(vector));
      assertTrue(e.getMessage().contains("The values in the offset buffer are decreasing"));
    }
  }

  @Test
  public void testStructVectorRangeEquals() {
    try (final StructVector vector = StructVector.empty("struct", allocator)) {
      IntVector intVector =
          vector.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      VarCharVector strVector =
          vector.addOrGet("f1", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);

      validateFull(vector);
      validateFull(intVector);
      validateFull(strVector);

      ValueVectorDataPopulator.setVector(intVector, 1, 2, 3, 4, 5);
      ValueVectorDataPopulator.setVector(strVector, "a", "b", "c", "d", "e");
      vector.setValueCount(5);

      validateFull(vector);
      validateFull(intVector);
      validateFull(strVector);

      ArrowBuf offsetBuf = strVector.getOffsetBuffer();
      offsetBuf.setInt(0, 100);
      offsetBuf.setInt(8, 50);

      ValidateUtility.ValidateException e = assertThrows(ValidateUtility.ValidateException.class,
          () -> validateFull(strVector));
      assertTrue(e.getMessage().contains("The values in the offset buffer are decreasing"));

      e = assertThrows(ValidateUtility.ValidateException.class,
          () -> validateFull(vector));
      assertTrue(e.getMessage().contains("The values in the offset buffer are decreasing"));
    }
  }
}

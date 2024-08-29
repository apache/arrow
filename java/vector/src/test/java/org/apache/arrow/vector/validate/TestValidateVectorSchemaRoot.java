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

import static org.apache.arrow.vector.util.ValueVectorUtility.validate;
import static org.apache.arrow.vector.util.ValueVectorUtility.validateFull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestValidateVectorSchemaRoot {

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
  public void testValidatePositive() {
    try (IntVector intVector = new IntVector("int vector", allocator);
         VarCharVector strVector = new VarCharVector("var char vector", allocator)) {

      VectorSchemaRoot root = VectorSchemaRoot.of(intVector, strVector);

      validate(root);
      validateFull(root);

      ValueVectorDataPopulator.setVector(intVector, 1, 2, 3, 4, 5);
      ValueVectorDataPopulator.setVector(strVector, "a", "b", "c", "d", "e");
      root.setRowCount(5);

      validate(root);
      validateFull(root);
    }
  }

  @Test
  public void testValidateNegative() {
    try (IntVector intVector = new IntVector("int vector", allocator);
         VarCharVector strVector = new VarCharVector("var char vector", allocator)) {

      VectorSchemaRoot root = VectorSchemaRoot.of(intVector, strVector);

      ValueVectorDataPopulator.setVector(intVector, 1, 2, 3, 4, 5);
      ValueVectorDataPopulator.setVector(strVector, "a", "b", "c", "d", "e");

      // validate mismatching value counts
      root.setRowCount(4);
      intVector.setValueCount(5);
      strVector.setValueCount(5);
      ValidateUtil.ValidateException e = assertThrows(ValidateUtil.ValidateException.class,
          () -> validate(root));
      assertTrue(e.getMessage().contains("Child vector and vector schema root have different value counts"));
      e = assertThrows(ValidateUtil.ValidateException.class,
          () -> validateFull(root));
      assertTrue(e.getMessage().contains("Child vector and vector schema root have different value counts"));

      // valid problems with the child vector
      root.setRowCount(5);
      ArrowBuf offsetBuf = strVector.getOffsetBuffer();
      offsetBuf.setInt(0, 100);
      offsetBuf.setInt(8, 50);
      validate(root);
      e = assertThrows(ValidateUtil.ValidateException.class,
          () -> validateFull(root));
      assertTrue(e.getMessage().contains("The values in positions 0 and 1 of the offset buffer are decreasing"));
    }
  }
}

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

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.RunEndEncodedVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType.RunEndEncoded;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestRunEndEncodedVector {

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
  public void testInitializeChildrenFromFields() throws Exception {
    final FieldType valueType = FieldType.notNullable(Types.MinorType.BIGINT.getType());
    final FieldType runEndType = FieldType.notNullable(Types.MinorType.INT.getType());

    final Field valueField = new Field("value", valueType, null);
    final Field runEndField = new Field("ree", runEndType, null);

    try (RunEndEncodedVector reeVector = RunEndEncodedVector.empty("empty", allocator)) {
      reeVector.initializeChildrenFromFields(List.of(runEndField, valueField));

      reeVector.validate();
    }
  }

  /** Create REE vector with constant value. */
  @Test
  public void testConstantValueVector() throws Exception {
    final FieldType valueType = FieldType.notNullable(Types.MinorType.BIGINT.getType());
    final FieldType runEndType = FieldType.notNullable(Types.MinorType.INT.getType());

    final Field valueField = new Field("value", valueType, null);
    final Field runEndField = new Field("ree", runEndType, null);
    final Field runEndEncodedField =
        new Field(
            "constant",
            FieldType.notNullable(RunEndEncoded.INSTANCE),
            List.of(runEndField, valueField));

    try (RunEndEncodedVector reeVector =
        new RunEndEncodedVector(runEndEncodedField, allocator, null)) {
      int runCount = 1;
      int logicalValueCount = 100;

      reeVector.allocateNew();
      reeVector.setInitialCapacity(runCount);
      ((BigIntVector) reeVector.getValuesVector()).set(0, 65536);
      ((IntVector) reeVector.getRunEndsVector()).set(0, logicalValueCount);
      reeVector.getValuesVector().setValueCount(runCount);
      reeVector.getRunEndsVector().setValueCount(runCount);
      reeVector.setValueCount(logicalValueCount);

      assertEquals(logicalValueCount, reeVector.getValueCount());
      for (int i = 0; i < logicalValueCount; i++) {
        assertEquals(65536L, reeVector.getObject(i));
      }
    }
  }

  /** Create REE vector representing: [1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5]. */
  @Test
  public void testBasicRunEndEncodedVector() throws Exception {

    final FieldType valueType = FieldType.notNullable(Types.MinorType.BIGINT.getType());
    final FieldType runEndType = FieldType.notNullable(Types.MinorType.INT.getType());

    final Field valueField = new Field("value", valueType, null);
    final Field runEndField = new Field("ree", runEndType, null);
    final Field runEndEncodedField =
        new Field(
            "ree", FieldType.notNullable(RunEndEncoded.INSTANCE), List.of(runEndField, valueField));

    try (RunEndEncodedVector reeVector =
        new RunEndEncodedVector(runEndEncodedField, allocator, null)) {
      int runCount = 5;
      reeVector.allocateNew();
      reeVector.setInitialCapacity(runCount);
      int end = 0;
      for (int i = 1; i <= runCount; i++) {
        end += i;
        ((BigIntVector) reeVector.getValuesVector()).set(i - 1, i);
        ((IntVector) reeVector.getRunEndsVector()).set(i - 1, end);
      }

      reeVector.getValuesVector().setValueCount(runCount);
      reeVector.getRunEndsVector().setValueCount(runCount);
      reeVector.setValueCount(end);

      assertEquals(15, reeVector.getValueCount());
      int index = 0;
      for (int i = 1; i < runCount + 1; i++) {
        for (int j = 0; j < i; j++) {
          assertEquals((long) i, reeVector.getObject(index));
          index++;
        }
      }
    }
  }
}

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.function.Function;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
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
  public void testInitializeChildrenFromFields() {
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
  public void testConstantValueVector() {
    final Field runEndEncodedField = createBigIntRunEndEncodedField("constant");
    int logicalValueCount = 100;

    // constant vector
    try (RunEndEncodedVector reeVector =
        new RunEndEncodedVector(runEndEncodedField, allocator, null)) {
      Long value = 65536L;
      setConstantVector(reeVector, value, logicalValueCount);
      assertEquals(logicalValueCount, reeVector.getValueCount());
      for (int i = 0; i < logicalValueCount; i++) {
        assertEquals(value, reeVector.getObject(i));
      }
    }

    // constant null vector
    try (RunEndEncodedVector reeVector =
        new RunEndEncodedVector(runEndEncodedField, allocator, null)) {
      setConstantVector(reeVector, null, logicalValueCount);
      assertEquals(logicalValueCount, reeVector.getValueCount());
      // Null count is always 0 for run-end encoded array
      assertEquals(0, reeVector.getNullCount());
      for (int i = 0; i < logicalValueCount; i++) {
        assertTrue(reeVector.isNull(i));
        assertNull(reeVector.getObject(i));
      }
    }
  }

  @Test
  public void testBasicRunEndEncodedVector() {
    try (RunEndEncodedVector reeVector =
        new RunEndEncodedVector(createBigIntRunEndEncodedField("basic"), allocator, null)) {

      // Create REE vector representing:
      // [null, 2, 2, null, null, null, 4, 4, 4, 4, null, null, null, null, null].
      int runCount = 5;
      final int logicalValueCount =
          setBasicVector(reeVector, runCount, i -> i % 2 == 0 ? null : i + 1, i -> i + 1);

      assertEquals(15, reeVector.getValueCount());
      int index = 0;
      for (int run = 0; run < runCount; run++) {
        long expectedRunValue = (long) run + 1;
        for (int j = 0; j <= run; j++) {
          if (run % 2 == 0) {
            assertNull(reeVector.getObject(index));
          } else {
            assertEquals(expectedRunValue, reeVector.getObject(index));
          }
          index++;
        }
      }

      // test index out of bound
      assertThrows(IndexOutOfBoundsException.class, () -> reeVector.getObject(-1));
      assertThrows(IndexOutOfBoundsException.class, () -> reeVector.getObject(logicalValueCount));
    }
  }

  @Test
  public void testRangeCompare() {
    // test compare same constant vector
    RunEndEncodedVector constantVector =
        new RunEndEncodedVector(createBigIntRunEndEncodedField("constant"), allocator, null);
    int logicalValueCount = 15;

    setConstantVector(constantVector, 1L, logicalValueCount);

    assertTrue(
        constantVector.accept(
            new RangeEqualsVisitor(constantVector, constantVector),
            new Range(0, 0, logicalValueCount)));
    assertTrue(
        constantVector.accept(
            new RangeEqualsVisitor(constantVector, constantVector), new Range(1, 1, 14)));
    assertTrue(
        constantVector.accept(
            new RangeEqualsVisitor(constantVector, constantVector), new Range(1, 2, 13)));
    assertFalse(
        constantVector.accept(
            new RangeEqualsVisitor(constantVector, constantVector), new Range(1, 10, 10)));
    assertFalse(
        constantVector.accept(
            new RangeEqualsVisitor(constantVector, constantVector), new Range(10, 1, 10)));

    // Create REE vector representing: [1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5].
    RunEndEncodedVector reeVector =
        new RunEndEncodedVector(createBigIntRunEndEncodedField("basic"), allocator, null);
    setBasicVector(reeVector, 5, i -> i + 1, i -> i + 1);

    assertTrue(
        reeVector.accept(
            new RangeEqualsVisitor(reeVector, reeVector), new Range(0, 0, logicalValueCount)));
    assertTrue(
        reeVector.accept(
            new RangeEqualsVisitor(reeVector, reeVector), new Range(2, 2, logicalValueCount - 2)));
    assertFalse(
        reeVector.accept(
            new RangeEqualsVisitor(reeVector, reeVector), new Range(1, 2, logicalValueCount - 2)));

    assertFalse(
        reeVector.accept(
            new RangeEqualsVisitor(reeVector, constantVector), new Range(0, 0, logicalValueCount)));

    // Create REE vector representing: [2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5].
    RunEndEncodedVector reeVector2 =
        new RunEndEncodedVector(createBigIntRunEndEncodedField("basic"), allocator, null);
    setBasicVector(reeVector2, 4, i -> i + 2, i -> i + 2);

    assertTrue(
        reeVector.accept(
            new RangeEqualsVisitor(reeVector, reeVector2), new Range(1, 0, logicalValueCount - 1)));

    constantVector.close();
    reeVector.close();
    reeVector2.close();
  }

  private static Field createBigIntRunEndEncodedField(String fieldName) {
    final FieldType valueType = FieldType.notNullable(Types.MinorType.BIGINT.getType());
    final FieldType runEndType = FieldType.notNullable(Types.MinorType.INT.getType());

    final Field valueField = new Field("value", valueType, null);
    final Field runEndField = new Field("ree", runEndType, null);

    return new Field(
        fieldName, FieldType.notNullable(RunEndEncoded.INSTANCE), List.of(runEndField, valueField));
  }

  private static void setConstantVector(
      RunEndEncodedVector constantVector, Long value, long logicalValueCount) {
    setBasicVector(constantVector, 1, i -> value, i -> logicalValueCount);
  }

  private static int setBasicVector(
      RunEndEncodedVector reeVector,
      int runCount,
      Function<Long, Long> runValueSupplier,
      Function<Long, Long> runLengthSupplier) {
    reeVector.allocateNew();
    reeVector.setInitialCapacity(runCount);
    int end = 0;
    for (int i = 0; i < runCount; i++) {
      Long runValue = runValueSupplier.apply((long) i);
      if (runValue == null) {
        reeVector.getValuesVector().setNull(i);
      } else {
        ((BigIntVector) reeVector.getValuesVector()).set(i, runValue);
      }

      Long runLength = runLengthSupplier.apply((long) i);
      assert runLength != null && runLength > 0;
      end += runLength;
      ((IntVector) reeVector.getRunEndsVector()).set(i, end);
    }

    final int logicalValueCount = end;
    reeVector.getValuesVector().setValueCount(runCount);
    reeVector.getRunEndsVector().setValueCount(runCount);
    reeVector.setValueCount(logicalValueCount);
    return logicalValueCount;
  }
}

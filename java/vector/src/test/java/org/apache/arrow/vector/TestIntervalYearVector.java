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
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIntervalYearVector {

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
  public void testGetAsStringBuilder() {
    try (final IntervalYearVector vector = new IntervalYearVector("", allocator)) {
      int valueCount = 100;
      vector.setInitialCapacity(valueCount);
      vector.allocateNew();
      for (int i = 0; i < valueCount; i++) {
        vector.set(i, i);
      }

      assertEquals("0 years 1 month ", vector.getAsStringBuilder(1).toString());
      assertEquals("0 years 10 months ", vector.getAsStringBuilder(10).toString());
      assertEquals("1 year 8 months ", vector.getAsStringBuilder(20).toString());
      assertEquals("2 years 6 months ", vector.getAsStringBuilder(30).toString());

      assertEquals(Types.MinorType.INTERVALYEAR, vector.getMinorType());
      ArrowType fieldType = vector.getField().getType();
      assertEquals(ArrowType.ArrowTypeID.Interval, fieldType.getTypeID());
      ArrowType.Interval intervalType = (ArrowType.Interval) fieldType;
      assertEquals(IntervalUnit.YEAR_MONTH, intervalType.getUnit());
    }
  }

  @Test
  public void testGetTransferPairWithField() {
    final IntervalYearVector fromVector = new IntervalYearVector("", allocator);
    final TransferPair transferPair = fromVector.getTransferPair(fromVector.getField(), allocator);
    final IntervalYearVector toVector = (IntervalYearVector) transferPair.getTo();
    // Field inside a new vector created by reusing a field should be the same in memory as the
    // original field.
    assertSame(fromVector.getField(), toVector.getField());
  }
}

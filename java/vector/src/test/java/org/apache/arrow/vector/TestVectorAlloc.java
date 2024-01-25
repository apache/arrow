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

import java.util.Arrays;
import java.util.Collections;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.rounding.DefaultRoundingPolicy;
import org.apache.arrow.memory.rounding.RoundingPolicy;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.Duration;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestVectorAlloc {
  private BufferAllocator rootAllocator;

  private BufferAllocator policyAllocator;

  @Before
  public void init() {
    rootAllocator = new RootAllocator(Long.MAX_VALUE);
    policyAllocator =
        new RootAllocator(AllocationListener.NOOP, Integer.MAX_VALUE, new CustomPolicy());
  }

  @After
  public void terminate() throws Exception {
    rootAllocator.close();
    policyAllocator.close();
  }

  private static Field field(String name, ArrowType type) {
    return new Field(name, new FieldType(true, type, null), Collections.emptyList());
  }

  @Test
  public void testVectorAllocWithField() {
    Schema schema = new Schema(Arrays.asList(
        field("TINYINT", MinorType.TINYINT.getType()),
        field("SMALLINT", MinorType.SMALLINT.getType()),
        field("INT", MinorType.INT.getType()),
        field("BIGINT", MinorType.BIGINT.getType()),
        field("UINT1", MinorType.UINT1.getType()),
        field("UINT2", MinorType.UINT2.getType()),
        field("UINT4", MinorType.UINT4.getType()),
        field("UINT8", MinorType.UINT8.getType()),
        field("FLOAT4", MinorType.FLOAT4.getType()),
        field("FLOAT8", MinorType.FLOAT8.getType()),
        field("UTF8", MinorType.VARCHAR.getType()),
        field("VARBINARY", MinorType.VARBINARY.getType()),
        field("BIT", MinorType.BIT.getType()),
        field("DECIMAL", new Decimal(38, 5, 128)),
        field("FIXEDSIZEBINARY", new FixedSizeBinary(50)),
        field("DATEDAY", MinorType.DATEDAY.getType()),
        field("DATEMILLI", MinorType.DATEMILLI.getType()),
        field("TIMESEC", MinorType.TIMESEC.getType()),
        field("TIMEMILLI", MinorType.TIMEMILLI.getType()),
        field("TIMEMICRO", MinorType.TIMEMICRO.getType()),
        field("TIMENANO", MinorType.TIMENANO.getType()),
        field("TIMESTAMPSEC", MinorType.TIMESTAMPSEC.getType()),
        field("TIMESTAMPMILLI", MinorType.TIMESTAMPMILLI.getType()),
        field("TIMESTAMPMICRO", MinorType.TIMESTAMPMICRO.getType()),
        field("TIMESTAMPNANO", MinorType.TIMESTAMPNANO.getType()),
        field("TIMESTAMPSECTZ", new Timestamp(TimeUnit.SECOND, "PST")),
        field("TIMESTAMPMILLITZ", new Timestamp(TimeUnit.MILLISECOND, "PST")),
        field("TIMESTAMPMICROTZ", new Timestamp(TimeUnit.MICROSECOND, "PST")),
        field("TIMESTAMPNANOTZ", new Timestamp(TimeUnit.NANOSECOND, "PST")),
        field("INTERVALDAY", MinorType.INTERVALDAY.getType()),
        field("INTERVALYEAR", MinorType.INTERVALYEAR.getType()),
        field("DURATION", new Duration(TimeUnit.MILLISECOND))
    ));

    try (BufferAllocator allocator = rootAllocator.newChildAllocator("child", 0, Long.MAX_VALUE)) {
      for (Field field : schema.getFields()) {
        try (FieldVector vector = field.createVector(allocator)) {
          assertEquals(vector.getMinorType(),
              Types.getMinorTypeForArrowType(field.getFieldType().getType()));
          vector.allocateNew();
        }
      }
    }
  }

  private static final int CUSTOM_SEGMENT_SIZE = 200;

  /**
   * A custom rounding policy that rounds the size to
   * the next multiple of 200.
   */
  private static class CustomPolicy implements RoundingPolicy {

    @Override
    public long getRoundedSize(long requestSize) {
      return (requestSize + CUSTOM_SEGMENT_SIZE - 1) / CUSTOM_SEGMENT_SIZE * CUSTOM_SEGMENT_SIZE;
    }
  }

  @Test
  public void testFixedWidthVectorAllocation() {
    try (IntVector vec1 = new IntVector("vec", policyAllocator);
        IntVector vec2 = new IntVector("vec", rootAllocator)) {
      assertTrue(vec1.getAllocator().getRoundingPolicy() instanceof CustomPolicy);
      vec1.allocateNew(50);
      long totalCapacity = vec1.getValidityBuffer().capacity() + vec1.getDataBuffer().capacity();

      // the total capacity must be a multiple of the segment size
      assertTrue(totalCapacity % CUSTOM_SEGMENT_SIZE == 0);

      assertTrue(vec2.getAllocator().getRoundingPolicy() instanceof DefaultRoundingPolicy);
      vec2.allocateNew(50);
      totalCapacity = vec2.getValidityBuffer().capacity() + vec2.getDataBuffer().capacity();

      // the total capacity must be a power of two
      assertEquals(0, totalCapacity & (totalCapacity - 1));
    }
  }

  @Test
  public void testVariableWidthVectorAllocation() {
    try (VarCharVector vec1 = new VarCharVector("vec", policyAllocator);
         VarCharVector vec2 = new VarCharVector("vec", rootAllocator)) {
      assertTrue(vec1.getAllocator().getRoundingPolicy() instanceof CustomPolicy);
      vec1.allocateNew(50);
      long totalCapacity = vec1.getValidityBuffer().capacity() + vec1.getOffsetBuffer().capacity();

      // the total capacity must be a multiple of the segment size
      assertTrue(totalCapacity % CUSTOM_SEGMENT_SIZE == 0);

      assertTrue(vec2.getAllocator().getRoundingPolicy() instanceof DefaultRoundingPolicy);
      vec2.allocateNew(50);
      totalCapacity = vec2.getValidityBuffer().capacity() + vec2.getOffsetBuffer().capacity();

      // the total capacity must be a power of two
      assertEquals(0, totalCapacity & (totalCapacity - 1));
    }
  }
}

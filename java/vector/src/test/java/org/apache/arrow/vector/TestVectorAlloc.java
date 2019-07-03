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

import java.util.Arrays;
import java.util.Collections;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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

  @Before
  public void init() {
    rootAllocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    rootAllocator.close();
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
        field("DECIMAL", new Decimal(38, 5)),
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
}

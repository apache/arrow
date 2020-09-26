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

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

public class TestTypeLayout {

  @Test
  public void testTypeBufferCount() {
    ArrowType type = new ArrowType.Int(8, true);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Union(UnionMode.Sparse, new int[2]);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Union(UnionMode.Dense, new int[1]);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Struct();
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.List();
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.FixedSizeList(5);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Map(false);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Decimal(10, 10, 128);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Decimal(10, 10, 256);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());


    type = new ArrowType.FixedSizeBinary(5);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Bool();
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Binary();
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Utf8();
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Null();
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Date(DateUnit.DAY);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Time(TimeUnit.MILLISECOND, 32);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Interval(IntervalUnit.DAY_TIME);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());

    type = new ArrowType.Duration(TimeUnit.MILLISECOND);
    assertEquals(TypeLayout.getTypeBufferCount(type), TypeLayout.getTypeLayout(type).getBufferLayouts().size());
  }
}

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

package org.apache.arrow.c;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.c.Flags;
import org.apache.arrow.c.Format;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

public class FormatTest {
  @Test
  public void testAsString() {
    assertEquals("z", Format.asString(new ArrowType.Binary()));
    assertEquals("b", Format.asString(new ArrowType.Bool()));
    assertEquals("tdD", Format.asString(new ArrowType.Date(DateUnit.DAY)));
    assertEquals("tdm", Format.asString(new ArrowType.Date(DateUnit.MILLISECOND)));
    assertEquals("d:1,1", Format.asString(new ArrowType.Decimal(1, 1, 128)));
    assertEquals("d:1,1,1", Format.asString(new ArrowType.Decimal(1, 1, 1)));
    assertEquals("d:9,1,1", Format.asString(new ArrowType.Decimal(9, 1, 1)));
    assertEquals("tDs", Format.asString(new ArrowType.Duration(TimeUnit.SECOND)));
    assertEquals("tDm", Format.asString(new ArrowType.Duration(TimeUnit.MILLISECOND)));
    assertEquals("tDu", Format.asString(new ArrowType.Duration(TimeUnit.MICROSECOND)));
    assertEquals("tDn", Format.asString(new ArrowType.Duration(TimeUnit.NANOSECOND)));
    assertEquals("w:1", Format.asString(new ArrowType.FixedSizeBinary(1)));
    assertEquals("+w:3", Format.asString(new ArrowType.FixedSizeList(3)));
    assertEquals("e", Format.asString(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)));
    assertEquals("f", Format.asString(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
    assertEquals("g", Format.asString(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    assertEquals("c", Format.asString(new ArrowType.Int(Byte.SIZE, true)));
    assertEquals("C", Format.asString(new ArrowType.Int(Byte.SIZE, false)));
    assertEquals("s", Format.asString(new ArrowType.Int(Short.SIZE, true)));
    assertEquals("S", Format.asString(new ArrowType.Int(Short.SIZE, false)));
    assertEquals("i", Format.asString(new ArrowType.Int(Integer.SIZE, true)));
    assertEquals("I", Format.asString(new ArrowType.Int(Integer.SIZE, false)));
    assertEquals("l", Format.asString(new ArrowType.Int(Long.SIZE, true)));
    assertEquals("L", Format.asString(new ArrowType.Int(Long.SIZE, false)));
    assertEquals("tiD", Format.asString(new ArrowType.Interval(IntervalUnit.DAY_TIME)));
    assertEquals("tiM", Format.asString(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)));
    assertEquals("Z", Format.asString(new ArrowType.LargeBinary()));
    assertEquals("+L", Format.asString(new ArrowType.LargeList()));
    assertEquals("U", Format.asString(new ArrowType.LargeUtf8()));
    assertEquals("+l", Format.asString(new ArrowType.List()));
    assertEquals("+m", Format.asString(new ArrowType.Map(true)));
    assertEquals("n", Format.asString(new ArrowType.Null()));
    assertEquals("+s", Format.asString(new ArrowType.Struct()));
    assertEquals("tts", Format.asString(new ArrowType.Time(TimeUnit.SECOND, 32)));
    assertEquals("ttm", Format.asString(new ArrowType.Time(TimeUnit.MILLISECOND, 32)));
    assertEquals("ttu", Format.asString(new ArrowType.Time(TimeUnit.MICROSECOND, 64)));
    assertEquals("ttn", Format.asString(new ArrowType.Time(TimeUnit.NANOSECOND, 64)));
    assertEquals("tss:Timezone", Format.asString(new ArrowType.Timestamp(TimeUnit.SECOND, "Timezone")));
    assertEquals("tsm:Timezone", Format.asString(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "Timezone")));
    assertEquals("tsu:Timezone", Format.asString(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Timezone")));
    assertEquals("tsn:Timezone", Format.asString(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "Timezone")));
    assertEquals("+us:1,1,1", Format.asString(new ArrowType.Union(UnionMode.Sparse, new int[] { 1, 1, 1 })));
    assertEquals("+ud:1,1,1", Format.asString(new ArrowType.Union(UnionMode.Dense, new int[] { 1, 1, 1 })));
    assertEquals("u", Format.asString(new ArrowType.Utf8()));

    assertThrows(UnsupportedOperationException.class, () -> Format.asString(new ArrowType.Int(1, true)));
    assertThrows(UnsupportedOperationException.class, () -> Format.asString(new ArrowType.Time(TimeUnit.SECOND, 1)));
    assertThrows(UnsupportedOperationException.class,
        () -> Format.asString(new ArrowType.Time(TimeUnit.MILLISECOND, 64)));
  }

  @Test
  public void testAsType() throws IllegalStateException, NumberFormatException, UnsupportedOperationException {
    assertTrue(Format.asType("n", 0L) instanceof ArrowType.Null);
    assertTrue(Format.asType("b", 0L) instanceof ArrowType.Bool);
    assertEquals(new ArrowType.Int(Byte.SIZE, true), Format.asType("c", 0L));
    assertEquals(new ArrowType.Int(Byte.SIZE, false), Format.asType("C", 0L));
    assertEquals(new ArrowType.Int(Short.SIZE, true), Format.asType("s", 0L));
    assertEquals(new ArrowType.Int(Short.SIZE, false), Format.asType("S", 0L));
    assertEquals(new ArrowType.Int(Integer.SIZE, true), Format.asType("i", 0L));
    assertEquals(new ArrowType.Int(Integer.SIZE, false), Format.asType("I", 0L));
    assertEquals(new ArrowType.Int(Long.SIZE, true), Format.asType("l", 0L));
    assertEquals(new ArrowType.Int(Long.SIZE, false), Format.asType("L", 0L));
    assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), Format.asType("e", 0L));
    assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), Format.asType("f", 0L));
    assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), Format.asType("g", 0L));
    assertTrue(Format.asType("z", 0L) instanceof ArrowType.Binary);
    assertTrue(Format.asType("Z", 0L) instanceof ArrowType.LargeBinary);
    assertTrue(Format.asType("u", 0L) instanceof ArrowType.Utf8);
    assertTrue(Format.asType("U", 0L) instanceof ArrowType.LargeUtf8);
    assertEquals(new ArrowType.Date(DateUnit.DAY), Format.asType("tdD", 0L));
    assertEquals(new ArrowType.Date(DateUnit.MILLISECOND), Format.asType("tdm", 0L));
    assertEquals(new ArrowType.Time(TimeUnit.SECOND, Integer.SIZE), Format.asType("tts", 0L));
    assertEquals(new ArrowType.Time(TimeUnit.MILLISECOND, Integer.SIZE), Format.asType("ttm", 0L));
    assertEquals(new ArrowType.Time(TimeUnit.MICROSECOND, Long.SIZE), Format.asType("ttu", 0L));
    assertEquals(new ArrowType.Time(TimeUnit.NANOSECOND, Long.SIZE), Format.asType("ttn", 0L));
    assertEquals(new ArrowType.Duration(TimeUnit.SECOND), Format.asType("tDs", 0L));
    assertEquals(new ArrowType.Duration(TimeUnit.MILLISECOND), Format.asType("tDm", 0L));
    assertEquals(new ArrowType.Duration(TimeUnit.MICROSECOND), Format.asType("tDu", 0L));
    assertEquals(new ArrowType.Duration(TimeUnit.NANOSECOND), Format.asType("tDn", 0L));
    assertEquals(new ArrowType.Interval(IntervalUnit.YEAR_MONTH), Format.asType("tiM", 0L));
    assertEquals(new ArrowType.Interval(IntervalUnit.DAY_TIME), Format.asType("tiD", 0L));
    assertTrue(Format.asType("+l", 0L) instanceof ArrowType.List);
    assertTrue(Format.asType("+L", 0L) instanceof ArrowType.LargeList);
    assertTrue(Format.asType("+s", 0L) instanceof ArrowType.Struct);
    assertEquals(new ArrowType.Map(false), Format.asType("+m", 0L));
    assertEquals(new ArrowType.Map(true), Format.asType("+m", Flags.ARROW_FLAG_MAP_KEYS_SORTED));
    assertEquals(new ArrowType.Decimal(1, 1, 128), Format.asType("d:1,1", 0L));
    assertEquals(new ArrowType.Decimal(1, 1, 1), Format.asType("d:1,1,1", 0L));
    assertEquals(new ArrowType.Decimal(9, 1, 1), Format.asType("d:9,1,1", 0L));
    assertEquals(new ArrowType.FixedSizeBinary(1), Format.asType("w:1", 0L));
    assertEquals(new ArrowType.FixedSizeList(3), Format.asType("+w:3", 0L));
    assertEquals(new ArrowType.Union(UnionMode.Dense, new int[] { 1, 1, 1 }), Format.asType("+ud:1,1,1", 0L));
    assertEquals(new ArrowType.Union(UnionMode.Sparse, new int[] { 1, 1, 1 }), Format.asType("+us:1,1,1", 0L));
    assertEquals(new ArrowType.Timestamp(TimeUnit.SECOND, "Timezone"), Format.asType("tss:Timezone", 0L));
    assertEquals(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "Timezone"), Format.asType("tsm:Timezone", 0L));
    assertEquals(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Timezone"), Format.asType("tsu:Timezone", 0L));
    assertEquals(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "Timezone"), Format.asType("tsn:Timezone", 0L));

    assertThrows(UnsupportedOperationException.class, () -> Format.asType("Format", 0L));
    assertThrows(UnsupportedOperationException.class, () -> Format.asType(":", 0L));
    assertThrows(NumberFormatException.class, () -> Format.asType("w:1,2,3", 0L));
  }
}

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

package org.apache.arrow.driver.jdbc.utils;

import static org.apache.arrow.driver.jdbc.utils.SqlTypes.getSqlTypeIdFromArrowType;
import static org.apache.arrow.driver.jdbc.utils.SqlTypes.getSqlTypeNameFromArrowType;
import static org.junit.Assert.assertEquals;

import java.sql.Types;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

public class SqlTypesTest {

  @Test
  public void testGetSqlTypeIdFromArrowType() {
    assertEquals(Types.TINYINT, getSqlTypeIdFromArrowType(new ArrowType.Int(8, true)));
    assertEquals(Types.SMALLINT, getSqlTypeIdFromArrowType(new ArrowType.Int(16, true)));
    assertEquals(Types.INTEGER, getSqlTypeIdFromArrowType(new ArrowType.Int(32, true)));
    assertEquals(Types.BIGINT, getSqlTypeIdFromArrowType(new ArrowType.Int(64, true)));

    assertEquals(Types.BINARY, getSqlTypeIdFromArrowType(new ArrowType.FixedSizeBinary(1024)));
    assertEquals(Types.VARBINARY, getSqlTypeIdFromArrowType(new ArrowType.Binary()));
    assertEquals(Types.LONGVARBINARY, getSqlTypeIdFromArrowType(new ArrowType.LargeBinary()));

    assertEquals(Types.VARCHAR, getSqlTypeIdFromArrowType(new ArrowType.Utf8()));
    assertEquals(Types.LONGVARCHAR, getSqlTypeIdFromArrowType(new ArrowType.LargeUtf8()));

    assertEquals(Types.DATE, getSqlTypeIdFromArrowType(new ArrowType.Date(DateUnit.MILLISECOND)));
    assertEquals(Types.TIME,
        getSqlTypeIdFromArrowType(new ArrowType.Time(TimeUnit.MILLISECOND, 32)));
    assertEquals(Types.TIMESTAMP,
        getSqlTypeIdFromArrowType(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "")));

    assertEquals(Types.BOOLEAN, getSqlTypeIdFromArrowType(new ArrowType.Bool()));

    assertEquals(Types.DECIMAL, getSqlTypeIdFromArrowType(new ArrowType.Decimal(0, 0, 64)));
    assertEquals(Types.DOUBLE,
        getSqlTypeIdFromArrowType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    assertEquals(Types.FLOAT,
        getSqlTypeIdFromArrowType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));

    assertEquals(Types.ARRAY, getSqlTypeIdFromArrowType(new ArrowType.List()));
    assertEquals(Types.ARRAY, getSqlTypeIdFromArrowType(new ArrowType.LargeList()));
    assertEquals(Types.ARRAY, getSqlTypeIdFromArrowType(new ArrowType.FixedSizeList(10)));

    assertEquals(Types.JAVA_OBJECT, getSqlTypeIdFromArrowType(new ArrowType.Struct()));
    assertEquals(Types.JAVA_OBJECT,
        getSqlTypeIdFromArrowType(new ArrowType.Duration(TimeUnit.MILLISECOND)));
    assertEquals(Types.JAVA_OBJECT,
        getSqlTypeIdFromArrowType(new ArrowType.Interval(IntervalUnit.DAY_TIME)));
    assertEquals(Types.JAVA_OBJECT,
        getSqlTypeIdFromArrowType(new ArrowType.Union(UnionMode.Dense, null)));
    assertEquals(Types.JAVA_OBJECT, getSqlTypeIdFromArrowType(new ArrowType.Map(true)));

    assertEquals(Types.NULL, getSqlTypeIdFromArrowType(new ArrowType.Null()));
  }

  @Test
  public void testGetSqlTypeNameFromArrowType() {
    assertEquals("TINYINT", getSqlTypeNameFromArrowType(new ArrowType.Int(8, true)));
    assertEquals("SMALLINT", getSqlTypeNameFromArrowType(new ArrowType.Int(16, true)));
    assertEquals("INTEGER", getSqlTypeNameFromArrowType(new ArrowType.Int(32, true)));
    assertEquals("BIGINT", getSqlTypeNameFromArrowType(new ArrowType.Int(64, true)));

    assertEquals("BINARY", getSqlTypeNameFromArrowType(new ArrowType.FixedSizeBinary(1024)));
    assertEquals("VARBINARY", getSqlTypeNameFromArrowType(new ArrowType.Binary()));
    assertEquals("LONGVARBINARY", getSqlTypeNameFromArrowType(new ArrowType.LargeBinary()));

    assertEquals("VARCHAR", getSqlTypeNameFromArrowType(new ArrowType.Utf8()));
    assertEquals("LONGVARCHAR", getSqlTypeNameFromArrowType(new ArrowType.LargeUtf8()));

    assertEquals("DATE", getSqlTypeNameFromArrowType(new ArrowType.Date(DateUnit.MILLISECOND)));
    assertEquals("TIME", getSqlTypeNameFromArrowType(new ArrowType.Time(TimeUnit.MILLISECOND, 32)));
    assertEquals("TIMESTAMP",
        getSqlTypeNameFromArrowType(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "")));

    assertEquals("BOOLEAN", getSqlTypeNameFromArrowType(new ArrowType.Bool()));

    assertEquals("DECIMAL", getSqlTypeNameFromArrowType(new ArrowType.Decimal(0, 0, 64)));
    assertEquals("DOUBLE",
        getSqlTypeNameFromArrowType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    assertEquals("FLOAT",
        getSqlTypeNameFromArrowType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));

    assertEquals("ARRAY", getSqlTypeNameFromArrowType(new ArrowType.List()));
    assertEquals("ARRAY", getSqlTypeNameFromArrowType(new ArrowType.LargeList()));
    assertEquals("ARRAY", getSqlTypeNameFromArrowType(new ArrowType.FixedSizeList(10)));

    assertEquals("JAVA_OBJECT", getSqlTypeNameFromArrowType(new ArrowType.Struct()));

    assertEquals("JAVA_OBJECT",
        getSqlTypeNameFromArrowType(new ArrowType.Duration(TimeUnit.MILLISECOND)));
    assertEquals("JAVA_OBJECT",
        getSqlTypeNameFromArrowType(new ArrowType.Interval(IntervalUnit.DAY_TIME)));
    assertEquals("JAVA_OBJECT",
        getSqlTypeNameFromArrowType(new ArrowType.Union(UnionMode.Dense, null)));
    assertEquals("JAVA_OBJECT", getSqlTypeNameFromArrowType(new ArrowType.Map(true)));

    assertEquals("NULL", getSqlTypeNameFromArrowType(new ArrowType.Null()));
  }
}

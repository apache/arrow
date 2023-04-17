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

package org.apache.arrow.driver.jdbc;

import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.util.Cursor;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link ArrowFlightJdbcCursor}.
 */
public class ArrowFlightJdbcCursorTest {

  ArrowFlightJdbcCursor cursor;
  BufferAllocator allocator;

  @After
  public void cleanUp() {
    allocator.close();
    cursor.close();
  }

  @Test
  public void testBinaryVectorNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("Binary", new ArrowType.Binary(), null);
    ((VarBinaryVector) root.getVector("Binary")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testDateVectorNullTrue() throws SQLException {
    final VectorSchemaRoot root =
        getVectorSchemaRoot("Date", new ArrowType.Date(DateUnit.DAY), null);
    ((DateDayVector) root.getVector("Date")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testDurationVectorNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("Duration",
        new ArrowType.Duration(TimeUnit.MILLISECOND), null);
    ((DurationVector) root.getVector("Duration")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testDateInternalNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("Interval",
        new ArrowType.Interval(IntervalUnit.DAY_TIME), null);
    ((IntervalDayVector) root.getVector("Interval")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testTimeStampVectorNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("TimeStamp",
        new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), null);
    ((TimeStampMilliVector) root.getVector("TimeStamp")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testTimeVectorNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("Time",
        new ArrowType.Time(TimeUnit.MILLISECOND, 32), null);
    ((TimeMilliVector) root.getVector("Time")).setNull(0);
    testCursorWasNull(root);

  }

  @Test
  public void testFixedSizeListVectorNullTrue() throws SQLException {
    List<Field> fieldList = new ArrayList<>();
    fieldList.add(new Field("Null", new FieldType(true, new ArrowType.Null(), null),
        null));
    final VectorSchemaRoot root = getVectorSchemaRoot("FixedSizeList",
        new ArrowType.FixedSizeList(10), fieldList);
    ((FixedSizeListVector) root.getVector("FixedSizeList")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testLargeListVectorNullTrue() throws SQLException {
    List<Field> fieldList = new ArrayList<>();
    fieldList.add(new Field("Null", new FieldType(true, new ArrowType.Null(), null),
        null));
    final VectorSchemaRoot root =
        getVectorSchemaRoot("LargeList", new ArrowType.LargeList(), fieldList);
    ((LargeListVector) root.getVector("LargeList")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testListVectorNullTrue() throws SQLException {
    List<Field> fieldList = new ArrayList<>();
    fieldList.add(new Field("Null", new FieldType(true, new ArrowType.Null(), null),
        null));
    final VectorSchemaRoot root = getVectorSchemaRoot("List", new ArrowType.List(), fieldList);
    ((ListVector) root.getVector("List")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testMapVectorNullTrue() throws SQLException {
    List<Field> structChildren = new ArrayList<>();
    structChildren.add(new Field("Key", new FieldType(false, new ArrowType.Utf8(), null),
        null));
    structChildren.add(new Field("Value", new FieldType(false, new ArrowType.Utf8(), null),
        null));
    List<Field> fieldList = new ArrayList<>();
    fieldList.add(new Field("Struct", new FieldType(false, new ArrowType.Struct(), null),
        structChildren));
    final VectorSchemaRoot root = getVectorSchemaRoot("Map", new ArrowType.Map(false), fieldList);
    ((MapVector) root.getVector("Map")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testStructVectorNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("Struct", new ArrowType.Struct(), null);
    ((StructVector) root.getVector("Struct")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testBaseIntVectorNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("BaseInt",
        new ArrowType.Int(32, false), null);
    ((UInt4Vector) root.getVector("BaseInt")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testBitVectorNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("Bit", new ArrowType.Bool(), null);
    ((BitVector) root.getVector("Bit")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testDecimalVectorNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("Decimal",
        new ArrowType.Decimal(2, 2, 128), null);
    ((DecimalVector) root.getVector("Decimal")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testFloat4VectorNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("Float4",
        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
    ((Float4Vector) root.getVector("Float4")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testFloat8VectorNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("Float8",
        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);
    ((Float8Vector) root.getVector("Float8")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testVarCharVectorNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("VarChar", new ArrowType.Utf8(), null);
    ((VarCharVector) root.getVector("VarChar")).setNull(0);
    testCursorWasNull(root);
  }

  @Test
  public void testNullVectorNullTrue() throws SQLException {
    final VectorSchemaRoot root = getVectorSchemaRoot("Null", new ArrowType.Null(), null);
    testCursorWasNull(root);
  }

  private VectorSchemaRoot getVectorSchemaRoot(String name, ArrowType arrowType,
                                               List<Field> children) {
    final Schema schema = new Schema(ImmutableList.of(
        new Field(
            name,
            new FieldType(true, arrowType,
                null),
            children)));
    allocator = new RootAllocator(Long.MAX_VALUE);
    final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    return root;
  }

  private void testCursorWasNull(VectorSchemaRoot root) throws SQLException {
    root.setRowCount(1);
    cursor = new ArrowFlightJdbcCursor(root);
    cursor.next();
    List<Cursor.Accessor> accessorList = cursor.createAccessors(null, null, null);
    accessorList.get(0).getObject();
    assertTrue(cursor.wasNull());
    root.close();
  }
}

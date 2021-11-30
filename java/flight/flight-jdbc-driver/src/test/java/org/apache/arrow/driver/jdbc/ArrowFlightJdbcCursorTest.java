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
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;

import com.google.common.collect.ImmutableList;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ArrowFlightJdbcCursor}.
 */
public class ArrowFlightJdbcCursorTest {

    ArrowFlightJdbcCursor cursor;

    @AfterEach
    public void cleanUp() {
        cursor.close();
    }

    @Test
    public void testBinaryVectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "Binary",
                        new FieldType(true, new ArrowType.Binary(),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((VarBinaryVector) root.getVector("Binary")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getBytes();
        assertTrue(cursor.wasNull());

    }

    @Test
    public void testDateVectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "Date",
                        new FieldType(true, new ArrowType.Date(DateUnit.DAY),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((DateDayVector) root.getVector("Date")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getDate(null);
        assertTrue(cursor.wasNull());
    }

    @Test
    public void testDurationVectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "Duration",
                        new FieldType(true, new ArrowType.Duration(TimeUnit.MILLISECOND),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((DurationVector) root.getVector("Duration")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());

    }

    @Test
    public void testDateInternalNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "Interval",
                        new FieldType(true, new ArrowType.Interval(IntervalUnit.DAY_TIME),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((IntervalDayVector) root.getVector("Interval")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());

    }

    @Test
    public void testTimeStampVectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "TimeStamp",
                        new FieldType(true, new ArrowType.Timestamp(TimeUnit.MILLISECOND,null),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((TimeStampMilliVector) root.getVector("TimeStamp")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());
    }

    @Test
    public void testTimeVectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "Time",
                        new FieldType(true, new ArrowType.Time(TimeUnit.MILLISECOND,32),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((TimeMilliVector) root.getVector("Time")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());

    }

    @Test
    public void testFixedSizeListVectorNullTrue() throws SQLException {
        List<Field> fieldList = new ArrayList<>();
        fieldList.add(new Field("Null", new FieldType(true,new ArrowType.Null(), null),
                null));
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "FixedSizeList",
                        new FieldType(true, new ArrowType.FixedSizeList(10),
                                null),
                        fieldList)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((FixedSizeListVector) root.getVector("FixedSizeList")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());
    }

    @Test
    public void testLargeListVectorNullTrue() throws SQLException {
        List<Field> fieldList = new ArrayList<>();
        fieldList.add(new Field("Null", new FieldType(true,new ArrowType.Null(), null),
                null));
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "LargeList",
                        new FieldType(true, new ArrowType.LargeList(),
                                null),
                        fieldList
                        )));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((LargeListVector) root.getVector("LargeList")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());
    }

    @Test
    public void testListVectorNullTrue() throws SQLException {
        List<Field> fieldList = new ArrayList<>();
        fieldList.add(new Field("Null", new FieldType(true,new ArrowType.Null(), null),
                null));
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "List",
                        new FieldType(true, new ArrowType.List(),
                                null),
                        fieldList)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((ListVector) root.getVector("List")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());
    }

    @Test
    public void testMapVectorNullTrue() throws SQLException {
        List<Field> structChildren = new ArrayList<>();
        structChildren.add(new Field("Key", new FieldType(false,new ArrowType.Utf8(), null),
                null));
        structChildren.add(new Field("Value", new FieldType(false,new ArrowType.Utf8(), null),
                null));
        List<Field> fieldList = new ArrayList<>();
        fieldList.add(new Field("Struct", new FieldType(false,new ArrowType.Struct(), null),
                structChildren));
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "Map",
                        new FieldType(true, new ArrowType.Map(false),
                                null),
                        fieldList)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((MapVector) root.getVector("Map")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());
    }

    @Test
    public void testStructVectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "Struct",
                        new FieldType(true, new ArrowType.Struct(),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((StructVector) root.getVector("Struct")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());

    }

    @Test
    public void testBaseIntVectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "BaseInt",
                        new FieldType(true, new ArrowType.Int(32,false),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((UInt4Vector) root.getVector("BaseInt")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());

    }

    @Test
    public void testBitVectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "Bit",
                        new FieldType(true, new ArrowType.Bool(),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((BitVector) root.getVector("Bit")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());
    }

    @Test
    public void testDecimalVectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "Decimal",
                        new FieldType(true, new ArrowType.Decimal(2, 2),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((DecimalVector) root.getVector("Decimal")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());

    }

    @Test
    public void testFloat4VectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "Float4",
                        new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((Float4Vector) root.getVector("Float4")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());

    }

    @Test
    public void testFloat8VectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "Float8",
                        new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((Float8Vector) root.getVector("Float8")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());

    }

    @Test
    public void testVarCharVectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "VarChar",
                        new FieldType(true, new ArrowType.Utf8(),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        ((VarCharVector) root.getVector("VarChar")).setNull(0);
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());

    }

    @Test
    public void testNullVectorNullTrue() throws SQLException {
        final Schema schema = new Schema(ImmutableList.of(
                new Field(
                        "Null",
                        new FieldType(true, new ArrowType.Null(),
                                null),
                        null)));
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        root.setRowCount(1);
        cursor = new ArrowFlightJdbcCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null,null,null);
        accessorList.get(0).getObject();
        assertTrue(cursor.wasNull());
    }
}

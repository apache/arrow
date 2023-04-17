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

package org.apache.arrow.adapter.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test options for dealing with unreliable ResultSetMetaData from JDBC drivers.
 */
@RunWith(Parameterized.class)
public class UnreliableMetaDataTest {
  private final boolean reuseVectorSchemaRoot;
  private BufferAllocator allocator;

  public UnreliableMetaDataTest(boolean reuseVectorSchemaRoot) {
    this.reuseVectorSchemaRoot = reuseVectorSchemaRoot;
  }

  @Before
  public void beforeEach() {
    allocator = new RootAllocator();
  }

  @After
  public void afterEach() {
    allocator.close();
  }

  @Parameterized.Parameters(name = "reuseVectorSchemaRoot = {0}")
  public static Collection<Object[]> getTestData() {
    return Arrays.asList(new Object[][] { {false}, {true} });
  }

  @Test
  public void testUnreliableMetaDataPrecisionAndScale() throws Exception {
    ResultSet rs = buildIncorrectPrecisionAndScaleMetaDataResultSet();
    ResultSetMetaData rsmd = rs.getMetaData();
    assertEquals("Column type should be Types.DECIMAL", Types.DECIMAL, rsmd.getColumnType(1));
    assertEquals("Column scale should be zero", 0, rsmd.getScale(1));
    assertEquals("Column precision should be zero", 0, rsmd.getPrecision(1));
    rs.next();
    BigDecimal bd1 = rs.getBigDecimal(1);
    assertEquals("Value should be 1000000000000000.01", new BigDecimal("1000000000000000.01"), bd1);
    assertEquals("Value scale should be 2", 2, bd1.scale());
    assertEquals("Value precision should be 18", 18, bd1.precision());
    assertFalse("No more rows!", rs.next());

    // reset the ResultSet:
    rs.beforeFirst();
    JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(
        allocator, JdbcToArrowUtils.getUtcCalendar(), /* include metadata */ false)
        .setReuseVectorSchemaRoot(reuseVectorSchemaRoot)
        .build();
    try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(rs, config)) {
      assertTrue(iter.hasNext());
      assertThrows(RuntimeException.class, iter::next, "Expected to fail due to mismatched metadata!");
    }

    // reset the ResultSet:
    rs.beforeFirst();
    JdbcFieldInfo explicitMappingField = new JdbcFieldInfo(Types.DECIMAL, 18, 2);
    Map<Integer, JdbcFieldInfo> explicitMapping = new HashMap<>();
    explicitMapping.put(1, explicitMappingField);
    config = new JdbcToArrowConfigBuilder(
        allocator, JdbcToArrowUtils.getUtcCalendar(), /* include metadata */ false)
        .setReuseVectorSchemaRoot(reuseVectorSchemaRoot)
        .setExplicitTypesByColumnIndex(explicitMapping)
        .build();

    try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(rs, config)) {
      while (iter.hasNext()) {
        VectorSchemaRoot root = iter.next();
        root.close();
      }
    }
  }

  @Test
  public void testInconsistentPrecisionAndScale() throws Exception {
    ResultSet rs = buildVaryingPrecisionAndScaleResultSet();
    ResultSetMetaData rsmd = rs.getMetaData();
    assertEquals("Column type should be Types.DECIMAL", Types.DECIMAL, rsmd.getColumnType(1));
    assertEquals("Column scale should be zero", 0, rsmd.getScale(1));
    assertEquals("Column precision should be zero", 0, rsmd.getPrecision(1));
    rs.next();
    BigDecimal bd1 = rs.getBigDecimal(1);
    assertEquals("Value should be 1000000000000000.01", new BigDecimal("1000000000000000.01"), bd1);
    assertEquals("Value scale should be 2", 2, bd1.scale());
    assertEquals("Value precision should be 18", 18, bd1.precision());
    rs.next();
    BigDecimal bd2 = rs.getBigDecimal(1);
    assertEquals("Value should be 1000000000300.0000001", new BigDecimal("1000000000300.0000001"), bd2);
    assertEquals("Value scale should be 7", 7, bd2.scale());
    assertEquals("Value precision should be 20", 20, bd2.precision());
    rs.beforeFirst();
    JdbcFieldInfo explicitMappingField = new JdbcFieldInfo(Types.DECIMAL, 20, 7);
    Map<Integer, JdbcFieldInfo> explicitMapping = new HashMap<>();
    explicitMapping.put(1, explicitMappingField);

    JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(
        allocator, JdbcToArrowUtils.getUtcCalendar(), /* include metadata */ false)
        .setReuseVectorSchemaRoot(reuseVectorSchemaRoot)
        .setExplicitTypesByColumnIndex(explicitMapping)
        .build();
    try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(rs, config)) {
      assertTrue(iter.hasNext());
      assertThrows(RuntimeException.class, iter::next,
          "This is expected to fail due to inconsistent BigDecimal scales, while strict matching is enabled.");
    }
    // Reuse same ResultSet, with RoundingMode.UNNECESSARY set to coerce BigDecmial scale as needed:
    config = new JdbcToArrowConfigBuilder(
        allocator, JdbcToArrowUtils.getUtcCalendar(), /* include metadata */ false)
        .setReuseVectorSchemaRoot(reuseVectorSchemaRoot)
        .setExplicitTypesByColumnIndex(explicitMapping)
        .setBigDecimalRoundingMode(RoundingMode.UNNECESSARY)
        .build();
    try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(rs, config)) {
      while (iter.hasNext()) {
        VectorSchemaRoot root = iter.next();
        root.close();
      }
    }
  }

  @Test
  public void testIncorrectNullability() throws Exception {
    // ARROW-17005: ResultSetMetaData may indicate a field is non-nullable even when there are nulls
    ResultSetUtility.MockResultSetMetaData.MockColumnMetaData columnMetaData =
        ResultSetUtility.MockResultSetMetaData.MockColumnMetaData.builder()
            .index(1)
            .sqlType(Types.INTEGER)
            .nullable(ResultSetMetaData.columnNoNulls)
            .build();
    ResultSetMetaData metadata = new ResultSetUtility.MockResultSetMetaData(Collections.singletonList(columnMetaData));
    final ResultSetUtility.MockResultSet.Builder resultSetBuilder = ResultSetUtility.MockResultSet.builder()
        .setMetaData(metadata)
        .addDataElement(new ResultSetUtility.MockDataElement(1024, Types.INTEGER))
        .finishRow()
        .addDataElement(new ResultSetUtility.MockDataElement(null, Types.INTEGER))
        .finishRow();
    final Schema notNullSchema = new Schema(
        Collections.singletonList(Field.notNullable(/*name=*/null, new ArrowType.Int(32, true))));
    final Schema nullSchema = new Schema(
        Collections.singletonList(Field.nullable(/*name=*/null, new ArrowType.Int(32, true))));

    try (final ResultSet rs = resultSetBuilder.build()) {
      JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(
          allocator, JdbcToArrowUtils.getUtcCalendar(), /* include metadata */ false)
          .setReuseVectorSchemaRoot(reuseVectorSchemaRoot)
          .build();
      try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(rs, config)) {
        assertTrue(iter.hasNext());
        final VectorSchemaRoot root = iter.next();
        // The wrong data is returned here
        assertEquals(notNullSchema, root.getSchema());
        assertEquals(2, root.getRowCount());
        final IntVector ints = (IntVector) root.getVector(0);
        assertEquals(1024, ints.get(0));
        assertFalse(ints.isNull(1));
        assertFalse(iter.hasNext());
        root.close();
      }

      rs.beforeFirst();

      // Override the nullability to get the correct result
      final Map<Integer, JdbcFieldInfo> typeMapping = new HashMap<>();
      JdbcFieldInfo realFieldInfo = new JdbcFieldInfo(
          Types.INTEGER, ResultSetMetaData.columnNullable, /*precision*/0, /*scale*/0);
      typeMapping.put(1, realFieldInfo);
      config = new JdbcToArrowConfigBuilder(
          allocator, JdbcToArrowUtils.getUtcCalendar(), /* include metadata */ false)
          .setReuseVectorSchemaRoot(reuseVectorSchemaRoot)
          .setExplicitTypesByColumnIndex(typeMapping)
          .build();
      try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(rs, config)) {
        assertTrue(iter.hasNext());
        final VectorSchemaRoot root = iter.next();
        assertEquals(nullSchema, root.getSchema());
        assertEquals(2, root.getRowCount());
        final IntVector ints = (IntVector) root.getVector(0);
        assertEquals(1024, ints.get(0));
        assertTrue(ints.isNull(1));
        assertFalse(iter.hasNext());
        root.close();
      }

      rs.beforeFirst();

      // columnNullableUnknown won't override the metadata
      realFieldInfo = new JdbcFieldInfo(
          Types.INTEGER, ResultSetMetaData.columnNullableUnknown, /*precision*/0, /*scale*/0);
      typeMapping.put(1, realFieldInfo);
      config = new JdbcToArrowConfigBuilder(
          allocator, JdbcToArrowUtils.getUtcCalendar(), /* include metadata */ false)
          .setReuseVectorSchemaRoot(reuseVectorSchemaRoot)
          .setExplicitTypesByColumnIndex(typeMapping)
          .build();
      try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(rs, config)) {
        assertTrue(iter.hasNext());
        final VectorSchemaRoot root = iter.next();
        assertEquals(notNullSchema, root.getSchema());
        assertEquals(2, root.getRowCount());
        final IntVector ints = (IntVector) root.getVector(0);
        assertEquals(1024, ints.get(0));
        assertFalse(ints.isNull(1));
        assertFalse(iter.hasNext());
        root.close();
      }
    }
  }

  private ResultSet buildIncorrectPrecisionAndScaleMetaDataResultSet() throws SQLException {
    ResultSetUtility.MockResultSetMetaData.MockColumnMetaData columnMetaData =
        ResultSetUtility.MockResultSetMetaData.MockColumnMetaData.builder()
            .index(1)
            .sqlType(Types.DECIMAL)
            .precision(0)
            .scale(0)
            .build();
    ArrayList<ResultSetUtility.MockResultSetMetaData.MockColumnMetaData> cols = new ArrayList<>();
    cols.add(columnMetaData);
    ResultSetMetaData metadata = new ResultSetUtility.MockResultSetMetaData(cols);
    return ResultSetUtility.MockResultSet.builder()
        .setMetaData(metadata)
        .addDataElement(
            new ResultSetUtility.MockDataElement(new BigDecimal("1000000000000000.01"), Types.DECIMAL)
        )
        .finishRow()
        .build();
  }

  private ResultSet buildVaryingPrecisionAndScaleResultSet() throws SQLException {
    ResultSetUtility.MockResultSetMetaData.MockColumnMetaData columnMetaData =
        ResultSetUtility.MockResultSetMetaData.MockColumnMetaData.builder()
            .index(1)
            .sqlType(Types.DECIMAL)
            .precision(0)
            .scale(0)
            .build();
    ArrayList<ResultSetUtility.MockResultSetMetaData.MockColumnMetaData> cols = new ArrayList<>();
    cols.add(columnMetaData);
    ResultSetMetaData metadata = new ResultSetUtility.MockResultSetMetaData(cols);
    return ResultSetUtility.MockResultSet.builder()
        .setMetaData(metadata)
        .addDataElement(
            new ResultSetUtility.MockDataElement(new BigDecimal("1000000000000000.01"), Types.DECIMAL)
        )
        .finishRow()
        .addDataElement(
            new ResultSetUtility.MockDataElement(new BigDecimal("1000000000300.0000001"), Types.DECIMAL)
        )
        .finishRow()
        .build();
  }
}

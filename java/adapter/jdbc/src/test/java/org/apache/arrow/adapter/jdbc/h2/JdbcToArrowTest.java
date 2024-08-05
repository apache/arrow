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
package org.apache.arrow.adapter.jdbc.h2;

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.stream.Stream;
import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.adapter.jdbc.ResultSetUtility;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * JUnit Test Class which contains methods to test JDBC to Arrow data conversion functionality with
 * various data types for H2 database using single test data file.
 */
public class JdbcToArrowTest extends AbstractJdbcToArrowTest {

  private static final String[] testFiles = {"h2/test1_all_datatypes_h2.yml"};

  /**
   * Get the test data as a collection of Table objects for each test iteration.
   *
   * @return Collection of Table objects
   * @throws SQLException on error
   * @throws ClassNotFoundException on error
   * @throws IOException on error
   */
  public static Stream<Arguments> getTestData()
      throws SQLException, ClassNotFoundException, IOException {
    return Arrays.stream(prepareTestData(testFiles, JdbcToArrowTest.class))
        .flatMap(row -> Stream.of(Arguments.of(row[0], true), Arguments.of(row[0], false)));
  }

  /**
   * Test Method to test JdbcToArrow Functionality for various H2 DB based datatypes with only one
   * test data file.
   */
  @ParameterizedTest
  @MethodSource("getTestData")
  public void testJdbcToArrowValues(Table table)
      throws SQLException, IOException, ClassNotFoundException {
    this.initializeDatabase(table);

    testDataSets(
        sqlToArrow(
            conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance()),
        false);
    testDataSets(sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE)), false);
    testDataSets(
        sqlToArrow(
            conn.createStatement().executeQuery(table.getQuery()),
            new RootAllocator(Integer.MAX_VALUE),
            Calendar.getInstance()),
        false);
    testDataSets(sqlToArrow(conn.createStatement().executeQuery(table.getQuery())), false);
    testDataSets(
        sqlToArrow(
            conn.createStatement().executeQuery(table.getQuery()),
            new RootAllocator(Integer.MAX_VALUE)),
        false);
    testDataSets(
        sqlToArrow(conn.createStatement().executeQuery(table.getQuery()), Calendar.getInstance()),
        false);
    Calendar calendar = Calendar.getInstance();
    ResultSetMetaData rsmd = getQueryMetaData(table.getQuery());
    testDataSets(
        sqlToArrow(
            conn.createStatement().executeQuery(table.getQuery()),
            new JdbcToArrowConfigBuilder(
                    new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance())
                .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
                .setJdbcToArrowTypeConverter(jdbcToArrowTypeConverter(calendar, rsmd))
                .build()),
        true);
    testDataSets(
        sqlToArrow(
            conn,
            table.getQuery(),
            new JdbcToArrowConfigBuilder(new RootAllocator(Integer.MAX_VALUE), calendar)
                .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
                .setJdbcToArrowTypeConverter(jdbcToArrowTypeConverter(calendar, rsmd))
                .build()),
        true);
  }

  @ParameterizedTest
  @MethodSource("getTestData")
  public void testJdbcSchemaMetadata(Table table, boolean reuseVectorSchemaRoot)
      throws SQLException, ClassNotFoundException {
    this.initializeDatabase(table);

    Calendar calendar = Calendar.getInstance();
    ResultSetMetaData rsmd = getQueryMetaData(table.getQuery());
    JdbcToArrowConfig config =
        new JdbcToArrowConfigBuilder(new RootAllocator(0), calendar, true)
            .setReuseVectorSchemaRoot(reuseVectorSchemaRoot)
            .setJdbcToArrowTypeConverter(jdbcToArrowTypeConverter(calendar, rsmd))
            .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
            .build();
    Schema schema = JdbcToArrowUtils.jdbcToArrowSchema(rsmd, config);
    JdbcToArrowTestHelper.assertFieldMetadataMatchesResultSetMetadata(rsmd, schema);
  }

  /**
   * This method calls the assert methods for various DataSets.
   *
   * @param root VectorSchemaRoot for test
   * @param isIncludeMapVector is this dataset checks includes map column. Jdbc type to 'map'
   *     mapping declared in configuration only manually
   */
  @Override
  public void testDataSets(VectorSchemaRoot root, boolean isIncludeMapVector) {
    JdbcToArrowTestHelper.assertFieldMetadataIsEmpty(root);
    assertBigIntVectorValues(
        (BigIntVector) root.getVector(BIGINT),
        table.getRowCount(),
        getLongValues(table.getValues(), BIGINT));

    assertTinyIntVectorValues(
        (TinyIntVector) root.getVector(TINYINT),
        table.getRowCount(),
        getIntValues(table.getValues(), TINYINT));

    assertSmallIntVectorValues(
        (SmallIntVector) root.getVector(SMALLINT),
        table.getRowCount(),
        getIntValues(table.getValues(), SMALLINT));

    assertVarBinaryVectorValues(
        (VarBinaryVector) root.getVector(BINARY),
        table.getRowCount(),
        getBinaryValues(table.getValues(), BINARY));

    assertVarBinaryVectorValues(
        (VarBinaryVector) root.getVector(BLOB),
        table.getRowCount(),
        getBinaryValues(table.getValues(), BLOB));

    assertVarcharVectorValues(
        (VarCharVector) root.getVector(CLOB),
        table.getRowCount(),
        getCharArray(table.getValues(), CLOB));

    assertVarcharVectorValues(
        (VarCharVector) root.getVector(VARCHAR),
        table.getRowCount(),
        getCharArray(table.getValues(), VARCHAR));

    assertVarcharVectorValues(
        (VarCharVector) root.getVector(CHAR),
        table.getRowCount(),
        getCharArray(table.getValues(), CHAR));

    assertIntVectorValues(
        (IntVector) root.getVector(INT), table.getRowCount(), getIntValues(table.getValues(), INT));

    assertBitVectorValues(
        (BitVector) root.getVector(BIT), table.getRowCount(), getIntValues(table.getValues(), BIT));

    assertBooleanVectorValues(
        (BitVector) root.getVector(BOOL),
        table.getRowCount(),
        getBooleanValues(table.getValues(), BOOL));

    assertDateVectorValues(
        (DateDayVector) root.getVector(DATE),
        table.getRowCount(),
        getIntValues(table.getValues(), DATE));

    assertTimeVectorValues(
        (TimeMilliVector) root.getVector(TIME),
        table.getRowCount(),
        getLongValues(table.getValues(), TIME));

    assertTimeStampVectorValues(
        (TimeStampVector) root.getVector(TIMESTAMP),
        table.getRowCount(),
        getLongValues(table.getValues(), TIMESTAMP));

    assertDecimalVectorValues(
        (DecimalVector) root.getVector(DECIMAL),
        table.getRowCount(),
        getDecimalValues(table.getValues(), DECIMAL));

    assertFloat8VectorValues(
        (Float8Vector) root.getVector(DOUBLE),
        table.getRowCount(),
        getDoubleValues(table.getValues(), DOUBLE));

    assertFloat4VectorValues(
        (Float4Vector) root.getVector(REAL),
        table.getRowCount(),
        getFloatValues(table.getValues(), REAL));

    assertNullVectorValues((NullVector) root.getVector(NULL), table.getRowCount());

    assertListVectorValues(
        (ListVector) root.getVector(LIST),
        table.getRowCount(),
        getListValues(table.getValues(), LIST));

    if (isIncludeMapVector) {
      assertMapVectorValues(
          (MapVector) root.getVector(MAP),
          table.getRowCount(),
          getMapValues(table.getValues(), MAP));
    }
  }

  @ParameterizedTest
  @MethodSource("getTestData")
  public void runLargeNumberOfRows(Table table, boolean reuseVectorSchemaRoot)
      throws IOException, SQLException, ClassNotFoundException {
    this.initializeDatabase(table);

    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    int x = 0;
    final int targetRows = 600000;
    ResultSet rs = ResultSetUtility.generateBasicResultSet(targetRows);
    JdbcToArrowConfig config =
        new JdbcToArrowConfigBuilder(
                allocator, JdbcToArrowUtils.getUtcCalendar(), /* include metadata */ false)
            .setReuseVectorSchemaRoot(reuseVectorSchemaRoot)
            .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
            .build();

    try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(rs, config)) {
      while (iter.hasNext()) {
        VectorSchemaRoot root = iter.next();
        x += root.getRowCount();
        if (!reuseVectorSchemaRoot) {
          root.close();
        }
      }
    } finally {
      allocator.close();
    }

    assertEquals(targetRows, x);
  }
}

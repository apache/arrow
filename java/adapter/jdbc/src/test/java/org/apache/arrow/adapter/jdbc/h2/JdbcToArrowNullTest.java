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

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertBigIntVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertBitVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertBooleanVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertDateVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertDecimalVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertFloat4VectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertFloat8VectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertIntVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertListVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertMapVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertNullValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertSmallIntVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTimeStampVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTimeVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTinyIntVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertVarBinaryVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertVarcharVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getBinaryValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getBooleanValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getCharArray;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getDecimalValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getDoubleValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getFloatValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getIntValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getListValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getLongValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getMapValues;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.stream.Stream;
import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
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
 * null values for H2 database.
 */
public class JdbcToArrowNullTest extends AbstractJdbcToArrowTest {

  private static final String NULL = "null";
  private static final String SELECTED_NULL_ROW = "selected_null_row";
  private static final String SELECTED_NULL_COLUMN = "selected_null_column";

  private static final String[] testFiles = {
    "h2/test1_all_datatypes_null_h2.yml",
    "h2/test1_selected_datatypes_null_h2.yml",
    "h2/test1_all_datatypes_selected_null_rows_h2.yml"
  };

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
    return Arrays.stream(prepareTestData(testFiles, JdbcToArrowNullTest.class)).map(Arguments::of);
  }

  /**
   * Test Method to test JdbcToArrow Functionality for various H2 DB based datatypes with null
   * values.
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
            new JdbcToArrowConfigBuilder(
                    new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance())
                .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
                .setJdbcToArrowTypeConverter(jdbcToArrowTypeConverter(calendar, rsmd))
                .build()),
        true);
  }

  @ParameterizedTest
  @MethodSource("getTestData")
  public void testJdbcSchemaMetadata(Table table) throws SQLException, ClassNotFoundException {
    this.initializeDatabase(table);

    JdbcToArrowConfig config =
        new JdbcToArrowConfigBuilder(new RootAllocator(0), Calendar.getInstance(), true)
            .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
            .build();
    ResultSetMetaData rsmd = conn.createStatement().executeQuery(table.getQuery()).getMetaData();
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

    switch (table.getType()) {
      case NULL:
        sqlToArrowTestNullValues(table.getVectors(), root, table.getRowCount());
        break;
      case SELECTED_NULL_COLUMN:
        sqlToArrowTestSelectedNullColumnsValues(
            table.getVectors(), root, table.getRowCount(), isIncludeMapVector);
        break;
      case SELECTED_NULL_ROW:
        testAllVectorValues(root, isIncludeMapVector);
        break;
      default:
        // do nothing
        break;
    }
  }

  private void testAllVectorValues(VectorSchemaRoot root, boolean isIncludeMapVector) {
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

  /**
   * This method assert tests null values in vectors for all the datatypes.
   *
   * @param vectors Vectors to test
   * @param root VectorSchemaRoot for test
   * @param rowCount number of rows
   */
  public void sqlToArrowTestNullValues(String[] vectors, VectorSchemaRoot root, int rowCount) {
    assertNullValues((IntVector) root.getVector(vectors[0]), rowCount);
    assertNullValues((BitVector) root.getVector(vectors[1]), rowCount);
    assertNullValues((TinyIntVector) root.getVector(vectors[2]), rowCount);
    assertNullValues((SmallIntVector) root.getVector(vectors[3]), rowCount);
    assertNullValues((BigIntVector) root.getVector(vectors[4]), rowCount);
    assertNullValues((DecimalVector) root.getVector(vectors[5]), rowCount);
    assertNullValues((Float8Vector) root.getVector(vectors[6]), rowCount);
    assertNullValues((Float4Vector) root.getVector(vectors[7]), rowCount);
    assertNullValues((TimeMilliVector) root.getVector(vectors[8]), rowCount);
    assertNullValues((DateDayVector) root.getVector(vectors[9]), rowCount);
    assertNullValues((TimeStampVector) root.getVector(vectors[10]), rowCount);
    assertNullValues((VarBinaryVector) root.getVector(vectors[11]), rowCount);
    assertNullValues((VarCharVector) root.getVector(vectors[12]), rowCount);
    assertNullValues((VarBinaryVector) root.getVector(vectors[13]), rowCount);
    assertNullValues((VarCharVector) root.getVector(vectors[14]), rowCount);
    assertNullValues((VarCharVector) root.getVector(vectors[15]), rowCount);
    assertNullValues((BitVector) root.getVector(vectors[16]), rowCount);
    assertNullValues((ListVector) root.getVector(vectors[17]), rowCount);
  }

  /**
   * This method assert tests null values in vectors for some selected datatypes.
   *
   * @param vectors Vectors to test
   * @param root VectorSchemaRoot for test
   * @param rowCount number of rows
   * @param isIncludeMapVector is this dataset checks includes map column. Jdbc type to 'map'
   *     mapping declared in configuration only manually
   */
  public void sqlToArrowTestSelectedNullColumnsValues(
      String[] vectors, VectorSchemaRoot root, int rowCount, boolean isIncludeMapVector) {
    assertNullValues((BigIntVector) root.getVector(vectors[0]), rowCount);
    assertNullValues((DecimalVector) root.getVector(vectors[1]), rowCount);
    assertNullValues((Float8Vector) root.getVector(vectors[2]), rowCount);
    assertNullValues((Float4Vector) root.getVector(vectors[3]), rowCount);
    assertNullValues((TimeMilliVector) root.getVector(vectors[4]), rowCount);
    assertNullValues((DateDayVector) root.getVector(vectors[5]), rowCount);
    assertNullValues((TimeStampVector) root.getVector(vectors[6]), rowCount);
    assertNullValues((VarBinaryVector) root.getVector(vectors[7]), rowCount);
    assertNullValues((VarCharVector) root.getVector(vectors[8]), rowCount);
    assertNullValues((VarBinaryVector) root.getVector(vectors[9]), rowCount);
    assertNullValues((VarCharVector) root.getVector(vectors[10]), rowCount);
    assertNullValues((VarCharVector) root.getVector(vectors[11]), rowCount);
    assertNullValues((BitVector) root.getVector(vectors[12]), rowCount);
    assertNullValues((ListVector) root.getVector(vectors[13]), rowCount);
    if (isIncludeMapVector) {
      assertNullValues((MapVector) root.getVector(vectors[14]), rowCount);
    }
  }
}

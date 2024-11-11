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
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertNullVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertSmallIntVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTimeStampVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTimeVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTinyIntVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertVarBinaryVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertVarcharVectorValues;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Calendar;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
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
import org.apache.arrow.vector.extension.OpaqueType;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * JUnit Test Class which contains methods to test JDBC to Arrow data conversion functionality with
 * various data types for H2 database using multiple test data files.
 */
public class JdbcToArrowDataTypesTest extends AbstractJdbcToArrowTest {

  private static final String BIGINT = "big_int";
  private static final String BINARY = "binary";
  private static final String BIT = "bit";
  private static final String BLOB = "blob";
  private static final String BOOL = "bool";
  private static final String CHAR = "char";
  private static final String CLOB = "clob";
  private static final String DATE = "date";
  private static final String DECIMAL = "decimal";
  private static final String DOUBLE = "double";
  private static final String INT = "int";
  private static final String LIST = "list";
  private static final String REAL = "real";
  private static final String SMALLINT = "small_int";
  private static final String TIME = "time";
  private static final String TIMESTAMP = "timestamp";
  private static final String TINYINT = "tiny_int";
  private static final String VARCHAR = "varchar";
  private static final String NULL = "null";

  private static final String[] testFiles = {
    "h2/test1_bigint_h2.yml",
    "h2/test1_binary_h2.yml",
    "h2/test1_bit_h2.yml",
    "h2/test1_blob_h2.yml",
    "h2/test1_bool_h2.yml",
    "h2/test1_char_h2.yml",
    "h2/test1_clob_h2.yml",
    "h2/test1_date_h2.yml",
    "h2/test1_decimal_h2.yml",
    "h2/test1_double_h2.yml",
    "h2/test1_int_h2.yml",
    "h2/test1_list_h2.yml",
    "h2/test1_real_h2.yml",
    "h2/test1_smallint_h2.yml",
    "h2/test1_time_h2.yml",
    "h2/test1_timestamp_h2.yml",
    "h2/test1_tinyint_h2.yml",
    "h2/test1_varchar_h2.yml",
    "h2/test1_null_h2.yml"
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
    return Arrays.stream(prepareTestData(testFiles, JdbcToArrowMapDataTypeTest.class))
        .map(Arguments::of);
  }

  /** Test Method to test JdbcToArrow Functionality for various H2 DB based datatypes. */
  @ParameterizedTest
  @MethodSource("getTestData")
  @Override
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
    testDataSets(
        sqlToArrow(
            conn.createStatement().executeQuery(table.getQuery()),
            new JdbcToArrowConfigBuilder(
                    new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance())
                .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
                .build()),
        false);
    testDataSets(
        sqlToArrow(
            conn,
            table.getQuery(),
            new JdbcToArrowConfigBuilder(
                    new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance())
                .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
                .build()),
        false);
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

  @Test
  void testOpaqueType() throws SQLException, ClassNotFoundException {
    try (BufferAllocator allocator = new RootAllocator()) {
      String url = "jdbc:h2:mem:JdbcToArrowTest";
      String driver = "org.h2.Driver";
      Class.forName(driver);
      conn = DriverManager.getConnection(url);
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("CREATE TABLE unknowntype (a GEOMETRY, b INT)");
      }

      String query = "SELECT * FROM unknowntype";
      Calendar calendar = Calendar.getInstance();
      Function<JdbcFieldInfo, ArrowType> typeConverter =
          (field) -> JdbcToArrowUtils.getArrowTypeFromJdbcType(field, calendar);
      JdbcToArrowConfig config =
          new JdbcToArrowConfigBuilder()
              .setAllocator(allocator)
              .setJdbcToArrowTypeConverter(
                  JdbcToArrowUtils.reportUnsupportedTypesAsOpaque(typeConverter, "H2"))
              .build();
      Schema schema;
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(query)) {
        schema =
            assertDoesNotThrow(() -> JdbcToArrowUtils.jdbcToArrowSchema(rs.getMetaData(), config));
      }

      Schema expected =
          new Schema(
              Arrays.asList(
                  Field.nullable(
                      "A", new OpaqueType(Types.MinorType.NULL.getType(), "GEOMETRY", "H2")),
                  Field.nullable("B", Types.MinorType.INT.getType())));
      assertEquals(expected, schema);
    }
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
      case BIGINT:
        assertBigIntVectorValues(
            (BigIntVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getLongValues());
        break;
      case BINARY:
      case BLOB:
        assertVarBinaryVectorValues(
            (VarBinaryVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getBinaryValues());
        break;
      case BIT:
        assertBitVectorValues(
            (BitVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getIntValues());
        break;
      case BOOL:
        assertBooleanVectorValues(
            (BitVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getBoolValues());
        break;
      case CHAR:
      case VARCHAR:
      case CLOB:
        assertVarcharVectorValues(
            (VarCharVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getCharValues());
        break;
      case DATE:
        assertDateVectorValues(
            (DateDayVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getIntValues());
        break;
      case TIME:
        assertTimeVectorValues(
            (TimeMilliVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getLongValues());
        break;
      case TIMESTAMP:
        assertTimeStampVectorValues(
            (TimeStampVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getLongValues());
        break;
      case DECIMAL:
        assertDecimalVectorValues(
            (DecimalVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getBigDecimalValues());
        break;
      case DOUBLE:
        assertFloat8VectorValues(
            (Float8Vector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getDoubleValues());
        break;
      case INT:
        assertIntVectorValues(
            (IntVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getIntValues());
        break;
      case SMALLINT:
        assertSmallIntVectorValues(
            (SmallIntVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getIntValues());
        break;
      case TINYINT:
        assertTinyIntVectorValues(
            (TinyIntVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getIntValues());
        break;
      case REAL:
        assertFloat4VectorValues(
            (Float4Vector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getFloatValues());
        break;
      case NULL:
        assertNullVectorValues((NullVector) root.getVector(table.getVector()), table.getRowCount());
        break;
      case LIST:
        assertListVectorValues(
            (ListVector) root.getVector(table.getVector()),
            table.getValues().length,
            table.getListValues());
        break;
      default:
        // do nothing
        break;
    }
  }
}

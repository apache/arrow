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
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getLongValues;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;

import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
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
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * JUnit Test Class which contains methods to test JDBC to Arrow data conversion functionality with various data types
 * for H2 database using single test data file.
 */
@RunWith(Parameterized.class)
public class JdbcToArrowTest extends AbstractJdbcToArrowTest {

  protected static final String BIGINT = "BIGINT_FIELD5";
  protected static final String BINARY = "BINARY_FIELD12";
  protected static final String BIT = "BIT_FIELD17";
  protected static final String BLOB = "BLOB_FIELD14";
  protected static final String BOOL = "BOOL_FIELD2";
  protected static final String CHAR = "CHAR_FIELD16";
  protected static final String CLOB = "CLOB_FIELD15";
  protected static final String DATE = "DATE_FIELD10";
  protected static final String DECIMAL = "DECIMAL_FIELD6";
  protected static final String DOUBLE = "DOUBLE_FIELD7";
  protected static final String INT = "INT_FIELD1";
  protected static final String REAL = "REAL_FIELD8";
  protected static final String SMALLINT = "SMALLINT_FIELD4";
  protected static final String TIME = "TIME_FIELD9";
  protected static final String TIMESTAMP = "TIMESTAMP_FIELD11";
  protected static final String TINYINT = "TINYINT_FIELD3";
  protected static final String VARCHAR = "VARCHAR_FIELD13";

  private static final String[] testFiles = {"h2/test1_all_datatypes_h2.yml"};

  /**
   * Constructor which populate table object for each test iteration.
   *
   * @param table Table object
   */
  public JdbcToArrowTest(Table table) {
    this.table = table;
  }

  /**
   * Get the test data as a collection of Table objects for each test iteration.
   *
   * @return Collection of Table objects
   * @throws SQLException on error
   * @throws ClassNotFoundException on error
   * @throws IOException on error
   */
  @Parameters
  public static Collection<Object[]> getTestData() throws SQLException, ClassNotFoundException, IOException {
    return Arrays.asList(prepareTestData(testFiles, JdbcToArrowTest.class));
  }

  /**
   * Test Method to test JdbcToArrow Functionality for various H2 DB based datatypes with only one test data file.
   */
  @Test
  public void testJdbcToArroValues() throws SQLException, IOException {
    testDataSets(JdbcToArrow.sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE),
        Calendar.getInstance()));
    testDataSets(JdbcToArrow.sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE)));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()),
        new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance()));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery())));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()),
        new RootAllocator(Integer.MAX_VALUE)));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()),
        Calendar.getInstance()));
    testDataSets(JdbcToArrow.sqlToArrow(
        conn.createStatement().executeQuery(table.getQuery()),
        new JdbcToArrowConfigBuilder(new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance()).build()));
    testDataSets(JdbcToArrow.sqlToArrow(
        conn,
        table.getQuery(),
        new JdbcToArrowConfigBuilder(new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance()).build()));
  }

  @Test
  public void testJdbcSchemaMetadata() throws SQLException {
    JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(new RootAllocator(0), Calendar.getInstance(), true).build();
    ResultSetMetaData rsmd = conn.createStatement().executeQuery(table.getQuery()).getMetaData();
    Schema schema = JdbcToArrowUtils.jdbcToArrowSchema(rsmd, config);
    JdbcToArrowTestHelper.assertFieldMetadataMatchesResultSetMetadata(rsmd, schema);
  }

  /**
   * This method calls the assert methods for various DataSets.
   *
   * @param root VectorSchemaRoot for test
   */
  public void testDataSets(VectorSchemaRoot root) {
    JdbcToArrowTestHelper.assertFieldMetadataIsEmpty(root);

    assertBigIntVectorValues((BigIntVector) root.getVector(BIGINT), table.getRowCount(),
            getLongValues(table.getValues(), BIGINT));

    assertTinyIntVectorValues((TinyIntVector) root.getVector(TINYINT), table.getRowCount(),
            getIntValues(table.getValues(), TINYINT));

    assertSmallIntVectorValues((SmallIntVector) root.getVector(SMALLINT), table.getRowCount(),
            getIntValues(table.getValues(), SMALLINT));

    assertVarBinaryVectorValues((VarBinaryVector) root.getVector(BINARY), table.getRowCount(),
            getBinaryValues(table.getValues(), BINARY));

    assertVarBinaryVectorValues((VarBinaryVector) root.getVector(BLOB), table.getRowCount(),
            getBinaryValues(table.getValues(), BLOB));

    assertVarcharVectorValues((VarCharVector) root.getVector(CLOB), table.getRowCount(),
            getCharArray(table.getValues(), CLOB));

    assertVarcharVectorValues((VarCharVector) root.getVector(VARCHAR), table.getRowCount(),
            getCharArray(table.getValues(), VARCHAR));

    assertVarcharVectorValues((VarCharVector) root.getVector(CHAR), table.getRowCount(),
            getCharArray(table.getValues(), CHAR));

    assertIntVectorValues((IntVector) root.getVector(INT), table.getRowCount(),
            getIntValues(table.getValues(), INT));

    assertBitVectorValues((BitVector) root.getVector(BIT), table.getRowCount(),
            getIntValues(table.getValues(), BIT));

    assertBooleanVectorValues((BitVector) root.getVector(BOOL), table.getRowCount(),
            getBooleanValues(table.getValues(), BOOL));

    assertDateVectorValues((DateMilliVector) root.getVector(DATE), table.getRowCount(),
            getLongValues(table.getValues(), DATE));

    assertTimeVectorValues((TimeMilliVector) root.getVector(TIME), table.getRowCount(),
            getLongValues(table.getValues(), TIME));

    assertTimeStampVectorValues((TimeStampVector) root.getVector(TIMESTAMP), table.getRowCount(),
            getLongValues(table.getValues(), TIMESTAMP));

    assertDecimalVectorValues((DecimalVector) root.getVector(DECIMAL), table.getRowCount(),
            getDecimalValues(table.getValues(), DECIMAL));

    assertFloat8VectorValues((Float8Vector) root.getVector(DOUBLE), table.getRowCount(),
            getDoubleValues(table.getValues(), DOUBLE));

    assertFloat4VectorValues((Float4Vector) root.getVector(REAL), table.getRowCount(),
            getFloatValues(table.getValues(), REAL));
  }

}

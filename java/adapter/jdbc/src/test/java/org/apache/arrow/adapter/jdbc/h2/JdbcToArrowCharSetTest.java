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

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertVarcharVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getCharArrayWithCharSet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * JUnit Test Class which contains methods to test JDBC to Arrow data conversion functionality with
 * UTF-8 Charset, including the multi-byte CJK characters for H2 database.
 */
public class JdbcToArrowCharSetTest extends AbstractJdbcToArrowTest {

  private static final String[] testFiles = {
    "h2/test1_charset_h2.yml",
    "h2/test1_charset_ch_h2.yml",
    "h2/test1_charset_jp_h2.yml",
    "h2/test1_charset_kr_h2.yml"
  };

  @Override
  public void initializeDatabase(Table table) throws SQLException, ClassNotFoundException {
    this.table = table;

    String url = "jdbc:h2:mem:JdbcToArrowTest?characterEncoding=UTF-8";
    String driver = "org.h2.Driver";
    Class.forName(driver);
    conn = DriverManager.getConnection(url);
    try (Statement stmt = conn.createStatement(); ) {
      stmt.executeUpdate(table.getCreate());
      for (String insert : table.getData()) {
        stmt.executeUpdate(insert);
      }
    }
  }

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
    return Arrays.stream(prepareTestData(testFiles, JdbcToArrowCharSetTest.class))
        .map(Arguments::of);
  }

  /**
   * Test Method to test JdbcToArrow Functionality for various H2 DB based datatypes with UTF-8
   * Charset, including the multi-byte CJK characters.
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
    testDataSets(
        sqlToArrow(
            conn.createStatement().executeQuery(table.getQuery()),
            new JdbcToArrowConfigBuilder(
                    new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance())
                .build()),
        false);
    testDataSets(
        sqlToArrow(
            conn,
            table.getQuery(),
            new JdbcToArrowConfigBuilder(
                    new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance())
                .build()),
        false);
  }

  @ParameterizedTest
  @MethodSource("getTestData")
  public void testJdbcSchemaMetadata(Table table) throws SQLException, ClassNotFoundException {
    this.initializeDatabase(table);

    JdbcToArrowConfig config =
        new JdbcToArrowConfigBuilder(new RootAllocator(0), Calendar.getInstance(), true).build();
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

    assertVarcharVectorValues(
        (VarCharVector) root.getVector(CLOB),
        table.getRowCount(),
        getCharArrayWithCharSet(table.getValues(), CLOB, StandardCharsets.UTF_8));

    assertVarcharVectorValues(
        (VarCharVector) root.getVector(VARCHAR),
        table.getRowCount(),
        getCharArrayWithCharSet(table.getValues(), VARCHAR, StandardCharsets.UTF_8));

    assertVarcharVectorValues(
        (VarCharVector) root.getVector(CHAR),
        table.getRowCount(),
        getCharArrayWithCharSet(table.getValues(), CHAR, StandardCharsets.UTF_8));
  }
}

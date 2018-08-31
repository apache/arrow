/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;

import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * JUnit Test Class which contains methods to test JDBC to Arrow data conversion functionality with UTF-8 Charset, including
 * the multi-byte CJK characters for H2 database
 */
@RunWith(Parameterized.class)
public class JdbcToArrowCharSetTest extends AbstractJdbcToArrowTest {
  private static final String VARCHAR = "VARCHAR_FIELD13";
  private static final String CHAR = "CHAR_FIELD16";
  private static final String CLOB = "CLOB_FIELD15";

  private static final String[] testFiles = {
          "h2/test1_charset_h2.yml",
          "h2/test1_charset_ch_h2.yml",
          "h2/test1_charset_jp_h2.yml",
          "h2/test1_charset_kr_h2.yml"
  };

  /**
   * Constructor which populate table object for each test iteration
   *
   * @param table
   */
  public JdbcToArrowCharSetTest(Table table) {
    this.table = table;
  }

  /**
   * This method creates Connection object and DB table and also populate data into table for test
   *
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  @Before
  public void setUp() throws SQLException, ClassNotFoundException {
    String url = "jdbc:h2:mem:JdbcToArrowTest?characterEncoding=UTF-8";
    String driver = "org.h2.Driver";
    Class.forName(driver);
    conn = DriverManager.getConnection(url);
    try (Statement stmt = conn.createStatement();) {
      stmt.executeUpdate(table.getCreate());
      for (String insert : table.getData()) {
        stmt.executeUpdate(insert);
      }
    }
  }

  /**
   * This method returns collection of Table object for each test iteration
   *
   * @return
   * @throws SQLException
   * @throws ClassNotFoundException
   * @throws IOException
   */
  @Parameters
  public static Collection<Object[]> getTestData() throws SQLException, ClassNotFoundException, IOException {
    return Arrays.asList(prepareTestData(testFiles, JdbcToArrowCharSetTest.class));
  }

  /**
   * Test Method to test JdbcToArrow Functionality for various H2 DB based datatypes with UTF-8 Charset, including
   * the multi-byte CJK characters
   */
  @Test
  public void testJdbcToArroValues() throws SQLException, IOException {
    testDataSets(JdbcToArrow.sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance()));
    testDataSets(JdbcToArrow.sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE)));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()), new RootAllocator(Integer.MAX_VALUE),
            Calendar.getInstance()));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery())));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()), new RootAllocator(Integer.MAX_VALUE)));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()), Calendar.getInstance()));
  }

  /**
   * This method calls the assert methods for various DataSets
   *
   * @param root
   */
  public void testDataSets(VectorSchemaRoot root) {
    assertVarcharVectorValues((VarCharVector) root.getVector(CLOB), table.getRowCount(),
            getCharArrayWithCharSet(table.getValues(), CLOB, StandardCharsets.UTF_8));

    assertVarcharVectorValues((VarCharVector) root.getVector(VARCHAR), table.getRowCount(),
            getCharArrayWithCharSet(table.getValues(), VARCHAR, StandardCharsets.UTF_8));

    assertVarcharVectorValues((VarCharVector) root.getVector(CHAR), table.getRowCount(),
            getCharArrayWithCharSet(table.getValues(), CHAR, StandardCharsets.UTF_8));
  }
}

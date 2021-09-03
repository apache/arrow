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

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JdbcAliasToArrowTest {
  private Connection conn = null;

  private static final String CREATE_STATEMENT =
      "CREATE TABLE example_table (id INTEGER);";
  private static final String INSERT_STATEMENT =
      "INSERT INTO example_table (id) VALUES (?);";
  private static final String QUERY = "SELECT id as a, id as b FROM example_table;";
  private static final String DROP_STATEMENT = "DROP TABLE example_table;";
  private static final String ORIGINAL_COLUMN_NAME = "ID";
  private static final String COLUMN_A = "A";
  private static final String COLUMN_B = "B";

  @Before
  public void setUp() throws Exception {
    String url = "jdbc:h2:mem:JdbcAliasToArrowTest";
    String driver = "org.h2.Driver";
    Class.forName(driver);
    conn = DriverManager.getConnection(url);
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(CREATE_STATEMENT);
    }
  }

  /**
   * Test h2 database query with alias for column name and column label.
   * To vetify reading field alias from an H2 database works as expected.
   * If this test fails, something is either wrong with the setup,
   * or the H2 SQL behavior changed.
   */
  @Test
  public void testReadH2Alias() throws Exception {
    // insert rows
    int rowCount = 4;
    insertRows(rowCount);

    try (ResultSet resultSet = conn.createStatement().executeQuery(QUERY)) {
      ResultSetMetaData rsmd = resultSet.getMetaData();
      assertEquals(2, rsmd.getColumnCount());

      // check column name and column label
      assertEquals(ORIGINAL_COLUMN_NAME, rsmd.getColumnName(1));
      assertEquals(COLUMN_A, rsmd.getColumnLabel(1));
      assertEquals(ORIGINAL_COLUMN_NAME, rsmd.getColumnName(2));
      assertEquals(COLUMN_B, rsmd.getColumnLabel(2));

      int rowNum = 0;

      while (resultSet.next()) {
        assertEquals(rowNum, resultSet.getInt(COLUMN_A));
        assertEquals(rowNum, resultSet.getInt(COLUMN_B));
        ++rowNum;
      }

      assertEquals(rowCount, rowNum);
    }
  }

  /**
   * Test jdbc query results with alias to arrow works expected.
   * Arrow result schema name should be field alias name.
   */
  @Test
  public void testJdbcAliasToArrow() throws Exception {
    int rowCount = 4;
    insertRows(rowCount);

    try (ResultSet resultSet = conn.createStatement().executeQuery(QUERY)) {
      final VectorSchemaRoot vector =
          JdbcToArrow.sqlToArrow(resultSet, new RootAllocator(Integer.MAX_VALUE));

      assertEquals(rowCount, vector.getRowCount());
      Schema vectorSchema = vector.getSchema();
      List<Field> vectorFields = vectorSchema.getFields();
      assertEquals(vectorFields.get(0).getName(), COLUMN_A);
      assertEquals(vectorFields.get(1).getName(), COLUMN_B);
    }
  }

  @After
  public void tearDown() throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(DROP_STATEMENT);
    } finally {
      if (conn != null) {
        conn.close();
        conn = null;
      }
    }
  }

  private void insertRows(int numRows) throws SQLException {
    // Insert [numRows] Rows
    try (PreparedStatement stmt = conn.prepareStatement(INSERT_STATEMENT)) {
      for (int i = 0; i < numRows; ++i) {
        stmt.setInt(1, i);
        stmt.executeUpdate();
      }
    }
  }
}

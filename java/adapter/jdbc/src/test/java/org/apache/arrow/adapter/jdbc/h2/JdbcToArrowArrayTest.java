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

import static org.junit.Assert.*;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class JdbcToArrowArrayTest {
  private Connection conn = null;

  private static final String CREATE_STATEMENT =
      "CREATE TABLE array_table (id INTEGER, int_array ARRAY, float_array ARRAY, string_array ARRAY);";
  private static final String INSERT_STATEMENT =
      "INSERT INTO array_table (id, int_array, float_array, string_array) VALUES (?, ?, ?, ?);";
  private static final String QUERY = "SELECT int_array, float_array, string_array FROM array_table ORDER BY id;";
  private static final String DROP_STATEMENT = "DROP TABLE array_table;";

  private static Map<String, JdbcFieldInfo> arrayFieldMapping;

  private static final String INT_ARRAY_FIELD_NAME = "INT_ARRAY";
  private static final String FLOAT_ARRAY_FIELD_NAME = "FLOAT_ARRAY";
  private static final String STRING_ARRAY_FIELD_NAME = "STRING_ARRAY";

  @Before
  public void setUp() throws Exception {
    String url = "jdbc:h2:mem:JdbcToArrowTest";
    String driver = "org.h2.Driver";
    Class.forName(driver);
    conn = DriverManager.getConnection(url);
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(CREATE_STATEMENT);
    }

    arrayFieldMapping = new HashMap<String, JdbcFieldInfo>();
    arrayFieldMapping.put(INT_ARRAY_FIELD_NAME, new JdbcFieldInfo(Types.INTEGER));
    arrayFieldMapping.put(FLOAT_ARRAY_FIELD_NAME, new JdbcFieldInfo(Types.REAL));
    arrayFieldMapping.put(STRING_ARRAY_FIELD_NAME, new JdbcFieldInfo(Types.VARCHAR));
  }

  // This test verifies reading an array field from an H2 database
  // works as expected.  If this test fails, something is either wrong
  // with the setup, or the H2 SQL behavior changed.
  @Test
  public void testReadH2Array() throws Exception {
    int rowCount = 4;

    Integer[][] intArrays = generateIntegerArrayField(rowCount);
    Float[][] floatArrays = generateFloatArrayField(rowCount);
    String[][] strArrays = generateStringArrayField(rowCount);

    insertRows(rowCount, intArrays, floatArrays, strArrays);

    try (ResultSet resultSet = conn.createStatement().executeQuery(QUERY)) {
      ResultSetMetaData rsmd = resultSet.getMetaData();
      assertEquals(3, rsmd.getColumnCount());

      for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
        assertEquals(Types.ARRAY, rsmd.getColumnType(i));
      }

      int rowNum = 0;

      while (resultSet.next()) {
        Array intArray = resultSet.getArray(INT_ARRAY_FIELD_NAME);
        assertFalse(resultSet.wasNull());
        try (ResultSet rs = intArray.getResultSet()) {
          int arrayIndex = 0;
          while (rs.next()) {
            assertEquals(intArrays[rowNum][arrayIndex].intValue(), rs.getInt(2));
            ++arrayIndex;
          }
          assertEquals(intArrays[rowNum].length, arrayIndex);
        }

        Array floatArray = resultSet.getArray(FLOAT_ARRAY_FIELD_NAME);
        assertFalse(resultSet.wasNull());
        try (ResultSet rs = floatArray.getResultSet()) {
          int arrayIndex = 0;
          while (rs.next()) {
            assertEquals(floatArrays[rowNum][arrayIndex].floatValue(), rs.getFloat(2), 0.001);
            ++arrayIndex;
          }
          assertEquals(floatArrays[rowNum].length, arrayIndex);
        }

        Array strArray = resultSet.getArray(STRING_ARRAY_FIELD_NAME);
        assertFalse(resultSet.wasNull());
        try (ResultSet rs = strArray.getResultSet()) {
          int arrayIndex = 0;
          while (rs.next()) {
            assertEquals(strArrays[rowNum][arrayIndex], rs.getString(2));
            ++arrayIndex;
          }
          assertEquals(strArrays[rowNum].length, arrayIndex);
        }

        ++rowNum;
      }

      assertEquals(rowCount, rowNum);
    }
  }

  @Test
  public void testJdbcToArrow() throws Exception {
    int rowCount = 4;

    Integer[][] intArrays = generateIntegerArrayField(rowCount);
    Float[][] floatArrays = generateFloatArrayField(rowCount);
    String[][] strArrays = generateStringArrayField(rowCount);

    insertRows(rowCount, intArrays, floatArrays, strArrays);

    final JdbcToArrowConfigBuilder builder =
        new JdbcToArrowConfigBuilder(new RootAllocator(Integer.MAX_VALUE), JdbcToArrowUtils.getUtcCalendar(), false);
    builder.setArraySubTypeByColumnNameMap(arrayFieldMapping);

    final JdbcToArrowConfig config = builder.build();

    try (ResultSet resultSet = conn.createStatement().executeQuery(QUERY)) {
      final VectorSchemaRoot vector = JdbcToArrow.sqlToArrow(resultSet, config);

      assertEquals(rowCount, vector.getRowCount());

      assertIntegerVectorEquals((ListVector) vector.getVector(INT_ARRAY_FIELD_NAME), rowCount, intArrays);
      assertFloatVectorEquals((ListVector) vector.getVector(FLOAT_ARRAY_FIELD_NAME), rowCount, floatArrays);
      assertStringVectorEquals((ListVector) vector.getVector(STRING_ARRAY_FIELD_NAME), rowCount, strArrays);
    }
  }

  @Test
  public void testJdbcToArrowWithNulls() throws Exception {
    int rowCount = 4;

    Integer[][] intArrays = {
        null,
        {0},
        {1},
        {},
    };

    Float[][] floatArrays = {
        { 2.0f },
        null,
        { 3.0f },
        {},
    };

    String[][] stringArrays = {
        {"4"},
        null,
        {"5"},
        {},
    };

    insertRows(rowCount, intArrays, floatArrays, stringArrays);

    final JdbcToArrowConfigBuilder builder =
        new JdbcToArrowConfigBuilder(new RootAllocator(Integer.MAX_VALUE), JdbcToArrowUtils.getUtcCalendar(), false);
    builder.setArraySubTypeByColumnNameMap(arrayFieldMapping);

    final JdbcToArrowConfig config = builder.build();

    try (ResultSet resultSet = conn.createStatement().executeQuery(QUERY)) {
      final VectorSchemaRoot vector = JdbcToArrow.sqlToArrow(resultSet, config);

      assertEquals(rowCount, vector.getRowCount());

      assertIntegerVectorEquals((ListVector) vector.getVector(INT_ARRAY_FIELD_NAME), rowCount, intArrays);
      assertFloatVectorEquals((ListVector) vector.getVector(FLOAT_ARRAY_FIELD_NAME), rowCount, floatArrays);
      assertStringVectorEquals((ListVector) vector.getVector(STRING_ARRAY_FIELD_NAME), rowCount, stringArrays);
    }
  }

  private void assertIntegerVectorEquals(ListVector listVector, int rowCount, Integer[][] expectedValues) {
    IntVector vector = (IntVector) listVector.getDataVector();
    ArrowBuf offsetBuffer = listVector.getOffsetBuffer();

    int prevOffset = 0;
    for (int row = 0; row < rowCount; ++row) {
      int offset = offsetBuffer.getInt((row + 1) * ListVector.OFFSET_WIDTH);

      if (expectedValues[row] == null) {
        assertEquals(0, listVector.isSet(row));
        assertEquals(0, offset - prevOffset);
        continue;
      }

      assertEquals(1, listVector.isSet(row));
      assertEquals(expectedValues[row].length, offset - prevOffset);

      for (int i = prevOffset; i < offset; ++i) {
        assertEquals(expectedValues[row][i - prevOffset].intValue(), vector.get(i));
      }

      prevOffset = offset;
    }
  }

  private void assertFloatVectorEquals(ListVector listVector, int rowCount, Float[][] expectedValues) {
    Float4Vector vector = (Float4Vector) listVector.getDataVector();
    ArrowBuf offsetBuffer = listVector.getOffsetBuffer();

    int prevOffset = 0;
    for (int row = 0; row < rowCount; ++row) {
      int offset = offsetBuffer.getInt((row + 1) * ListVector.OFFSET_WIDTH);

      if (expectedValues[row] == null) {
        assertEquals(0, listVector.isSet(row));
        assertEquals(0, offset - prevOffset);
        continue;
      }

      assertEquals(1, listVector.isSet(row));
      assertEquals(expectedValues[row].length, offset - prevOffset);

      for (int i = prevOffset; i < offset; ++i) {
        assertEquals(expectedValues[row][i - prevOffset].floatValue(), vector.get(i), 0);
      }

      prevOffset = offset;
    }
  }

  private void assertStringVectorEquals(ListVector listVector, int rowCount, String[][] expectedValues) {
    VarCharVector vector = (VarCharVector) listVector.getDataVector();
    ArrowBuf offsetBuffer = listVector.getOffsetBuffer();

    int prevOffset = 0;
    for (int row = 0; row < rowCount; ++row) {
      int offset = offsetBuffer.getInt((row + 1) * ListVector.OFFSET_WIDTH);

      if (expectedValues[row] == null) {
        assertEquals(0, listVector.isSet(row));
        assertEquals(0, offset - prevOffset);
        continue;
      }

      assertEquals(1, listVector.isSet(row));
      assertEquals(expectedValues[row].length, offset - prevOffset);
      for (int i = prevOffset; i < offset; ++i) {
        assertArrayEquals(expectedValues[row][i - prevOffset].getBytes(), vector.get(i));
      }

      prevOffset = offset;
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

  private Integer[][] generateIntegerArrayField(int numRows) {
    Integer[][] result = new Integer[numRows][];

    for (int i = 0; i < numRows; ++i) {
      int val = i * 4;
      result[i] = new Integer[]{val, val + 1, val + 2, val + 3};
    }

    return result;
  }

  private Float[][] generateFloatArrayField(int numRows) {
    Float[][] result = new Float[numRows][];
  
    for (int i = 0; i < numRows; ++i) {
      int val = i * 4;
      result[i] = new Float[]{(float) val, (float) val + 1, (float) val + 2, (float) val + 3};
    }

    return result;
  }

  private String[][] generateStringArrayField(int numRows) {
    String[][] result = new String[numRows][];

    for (int i = 0; i < numRows; ++i) {
      int val = i * 4;
      result[i] = new String[]{
          String.valueOf(val),
          String.valueOf(val + 1),
          String.valueOf(val + 2),
          String.valueOf(val + 3) };
    }

    return result;
  }

  private void insertRows(
      int numRows,
      Integer[][] integerArrays,
      Float[][] floatArrays,
      String[][] strArrays)
          throws SQLException {

    // Insert 4 Rows
    try (PreparedStatement stmt = conn.prepareStatement(INSERT_STATEMENT)) {

      for (int i = 0; i < numRows; ++i) {
        Integer[] integerArray = integerArrays[i];
        Float[] floatArray = floatArrays[i];
        String[] strArray = strArrays[i];

        Array intArray = conn.createArrayOf("INT", integerArray);
        Array realArray = conn.createArrayOf("REAL", floatArray);
        Array varcharArray = conn.createArrayOf("VARCHAR", strArray);

        // Insert Arrays of 4 Values in Each Row
        stmt.setInt(1, i);
        stmt.setArray(2, intArray);
        stmt.setArray(3, realArray);
        stmt.setArray(4, varcharArray);

        stmt.executeUpdate();

        intArray.free();
        realArray.free();
        varcharArray.free();
      }
    }
  }
}

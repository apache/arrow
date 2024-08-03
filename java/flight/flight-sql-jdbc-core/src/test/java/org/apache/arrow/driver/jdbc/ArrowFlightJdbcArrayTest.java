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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ArrowFlightJdbcArrayTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  IntVector dataVector;

  @BeforeEach
  public void setup() {
    dataVector = rootAllocatorTestExtension.createIntVector();
  }

  @AfterEach
  public void tearDown() {
    this.dataVector.close();
  }

  @Test
  public void testShouldGetBaseTypeNameReturnCorrectTypeName() {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    assertEquals("INTEGER", arrowFlightJdbcArray.getBaseTypeName());
  }

  @Test
  public void testShouldGetBaseTypeReturnCorrectType() {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    assertEquals(Types.INTEGER, arrowFlightJdbcArray.getBaseType());
  }

  @Test
  public void testShouldGetArrayReturnValidArray() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    Object[] array = (Object[]) arrowFlightJdbcArray.getArray();

    Object[] expected = new Object[dataVector.getValueCount()];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = dataVector.getObject(i);
    }
    assertArrayEquals(array, expected);
  }

  @Test
  public void testShouldGetArrayReturnValidArrayWithOffsets() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    Object[] array = (Object[]) arrowFlightJdbcArray.getArray(1, 5);

    Object[] expected = new Object[5];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = dataVector.getObject(i + 1);
    }
    assertArrayEquals(array, expected);
  }

  @Test
  public void testShouldGetArrayWithOffsetsThrowArrayIndexOutOfBoundsException()
      throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> arrowFlightJdbcArray.getArray(0, dataVector.getValueCount() + 1));
  }

  @Test
  public void testShouldGetArrayWithMapNotBeSupported() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    HashMap<String, Class<?>> map = new HashMap<>();
    assertThrows(SQLFeatureNotSupportedException.class, () -> arrowFlightJdbcArray.getArray(map));
  }

  @Test
  public void testShouldGetArrayWithOffsetsAndMapNotBeSupported() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    HashMap<String, Class<?>> map = new HashMap<>();
    assertThrows(
        SQLFeatureNotSupportedException.class, () -> arrowFlightJdbcArray.getArray(0, 5, map));
  }

  @Test
  public void testShouldGetResultSetReturnValidResultSet() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    try (ResultSet resultSet = arrowFlightJdbcArray.getResultSet()) {
      int count = 0;
      while (resultSet.next()) {
        assertEquals((Object) resultSet.getInt(1), dataVector.getObject(count));
        count++;
      }
    }
  }

  @Test
  public void testShouldGetResultSetReturnValidResultSetWithOffsets() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    try (ResultSet resultSet = arrowFlightJdbcArray.getResultSet(3, 5)) {
      int count = 0;
      while (resultSet.next()) {
        assertEquals((Object) resultSet.getInt(1), dataVector.getObject(count + 3));
        count++;
      }
      assertEquals(5, count);
    }
  }

  @Test
  public void testToString() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());

    JsonStringArrayList<Object> array = new JsonStringArrayList<>();
    array.addAll(Arrays.asList((Object[]) arrowFlightJdbcArray.getArray()));

    assertEquals(array.toString(), arrowFlightJdbcArray.toString());
  }

  @Test
  public void testShouldGetResultSetWithMapNotBeSupported() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    HashMap<String, Class<?>> map = new HashMap<>();
    assertThrows(
        SQLFeatureNotSupportedException.class, () -> arrowFlightJdbcArray.getResultSet(map));
  }

  @Test
  public void testShouldGetResultSetWithOffsetsAndMapNotBeSupported() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    HashMap<String, Class<?>> map = new HashMap<>();
    assertThrows(
        SQLFeatureNotSupportedException.class, () -> arrowFlightJdbcArray.getResultSet(0, 5, map));
  }
}

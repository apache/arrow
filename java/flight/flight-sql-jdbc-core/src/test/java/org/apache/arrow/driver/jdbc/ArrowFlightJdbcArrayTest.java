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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ArrowFlightJdbcArrayTest {

  @Rule
  public RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  IntVector dataVector;

  @Before
  public void setup() {
    dataVector = rootAllocatorTestRule.createIntVector();
  }

  @After
  public void tearDown() {
    this.dataVector.close();
  }

  @Test
  public void testShouldGetBaseTypeNameReturnCorrectTypeName() {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    Assert.assertEquals("INTEGER", arrowFlightJdbcArray.getBaseTypeName());
  }

  @Test
  public void testShouldGetBaseTypeReturnCorrectType() {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    Assert.assertEquals(Types.INTEGER, arrowFlightJdbcArray.getBaseType());
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
    Assert.assertArrayEquals(array, expected);
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
    Assert.assertArrayEquals(array, expected);
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testShouldGetArrayWithOffsetsThrowArrayIndexOutOfBoundsException()
      throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    arrowFlightJdbcArray.getArray(0, dataVector.getValueCount() + 1);
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testShouldGetArrayWithMapNotBeSupported() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    HashMap<String, Class<?>> map = new HashMap<>();
    arrowFlightJdbcArray.getArray(map);
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testShouldGetArrayWithOffsetsAndMapNotBeSupported() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    HashMap<String, Class<?>> map = new HashMap<>();
    arrowFlightJdbcArray.getArray(0, 5, map);
  }

  @Test
  public void testShouldGetResultSetReturnValidResultSet() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    try (ResultSet resultSet = arrowFlightJdbcArray.getResultSet()) {
      int count = 0;
      while (resultSet.next()) {
        Assert.assertEquals((Object) resultSet.getInt(1), dataVector.getObject(count));
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
        Assert.assertEquals((Object) resultSet.getInt(1), dataVector.getObject(count + 3));
        count++;
      }
      Assert.assertEquals(count, 5);
    }
  }

  @Test
  public void testToString() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());

    JsonStringArrayList<Object> array = new JsonStringArrayList<>();
    array.addAll(Arrays.asList((Object[]) arrowFlightJdbcArray.getArray()));

    Assert.assertEquals(array.toString(), arrowFlightJdbcArray.toString());
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testShouldGetResultSetWithMapNotBeSupported() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    HashMap<String, Class<?>> map = new HashMap<>();
    arrowFlightJdbcArray.getResultSet(map);
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testShouldGetResultSetWithOffsetsAndMapNotBeSupported() throws SQLException {
    ArrowFlightJdbcArray arrowFlightJdbcArray =
        new ArrowFlightJdbcArray(dataVector, 0, dataVector.getValueCount());
    HashMap<String, Class<?>> map = new HashMap<>();
    arrowFlightJdbcArray.getResultSet(0, 5, map);
  }
}

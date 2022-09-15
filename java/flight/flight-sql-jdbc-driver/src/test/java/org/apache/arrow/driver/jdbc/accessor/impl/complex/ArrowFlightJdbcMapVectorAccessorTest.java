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

package org.apache.arrow.driver.jdbc.accessor.impl.complex;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightJdbcMapVectorAccessorTest {
  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private MapVector vector;

  @Before
  public void setup() {
    vector = MapVector.empty("", rootAllocatorTestRule.getRootAllocator(), false);
    UnionMapWriter writer = vector.getWriter();
    writer.allocate();
    writer.setPosition(0); // optional
    writer.startMap();
    writer.startEntry();
    writer.key().integer().writeInt(1);
    writer.value().integer().writeInt(11);
    writer.endEntry();
    writer.startEntry();
    writer.key().integer().writeInt(2);
    writer.value().integer().writeInt(22);
    writer.endEntry();
    writer.startEntry();
    writer.key().integer().writeInt(3);
    writer.value().integer().writeInt(33);
    writer.endEntry();
    writer.endMap();

    writer.setPosition(1);
    writer.startMap();
    writer.startEntry();
    writer.key().integer().writeInt(2);
    writer.endEntry();
    writer.endMap();

    writer.setPosition(2);
    writer.startMap();
    writer.startEntry();
    writer.key().integer().writeInt(0);
    writer.value().integer().writeInt(2000);
    writer.endEntry();
    writer.startEntry();
    writer.key().integer().writeInt(1);
    writer.value().integer().writeInt(2001);
    writer.endEntry();
    writer.startEntry();
    writer.key().integer().writeInt(2);
    writer.value().integer().writeInt(2002);
    writer.endEntry();
    writer.startEntry();
    writer.key().integer().writeInt(3);
    writer.value().integer().writeInt(2003);
    writer.endEntry();
    writer.endMap();

    writer.setValueCount(3);
  }

  @After
  public void tearDown() {
    vector.close();
  }

  @Test
  public void testShouldGetObjectReturnValidMap() {
    AccessorTestUtils.Cursor cursor = new AccessorTestUtils.Cursor(vector.getValueCount());
    ArrowFlightJdbcMapVectorAccessor accessor =
        new ArrowFlightJdbcMapVectorAccessor(vector, cursor::getCurrentRow, (boolean wasNull) -> {
        });

    Map<Object, Object> expected = new JsonStringHashMap<>();
    expected.put(1, 11);
    expected.put(2, 22);
    expected.put(3, 33);
    Assert.assertEquals(expected, accessor.getObject());
    Assert.assertFalse(accessor.wasNull());

    cursor.next();
    expected = new JsonStringHashMap<>();
    expected.put(2, null);
    Assert.assertEquals(expected, accessor.getObject());
    Assert.assertFalse(accessor.wasNull());

    cursor.next();
    expected = new JsonStringHashMap<>();
    expected.put(0, 2000);
    expected.put(1, 2001);
    expected.put(2, 2002);
    expected.put(3, 2003);
    Assert.assertEquals(expected, accessor.getObject());
    Assert.assertFalse(accessor.wasNull());
  }

  @Test
  public void testShouldGetObjectReturnNull() {
    vector.setNull(0);
    ArrowFlightJdbcMapVectorAccessor accessor =
        new ArrowFlightJdbcMapVectorAccessor(vector, () -> 0, (boolean wasNull) -> {
        });

    Assert.assertNull(accessor.getObject());
    Assert.assertTrue(accessor.wasNull());
  }

  @Test
  public void testShouldGetArrayReturnValidArray() throws SQLException {
    AccessorTestUtils.Cursor cursor = new AccessorTestUtils.Cursor(vector.getValueCount());
    ArrowFlightJdbcMapVectorAccessor accessor =
        new ArrowFlightJdbcMapVectorAccessor(vector, cursor::getCurrentRow, (boolean wasNull) -> {
        });

    Array array = accessor.getArray();
    Assert.assertNotNull(array);
    Assert.assertFalse(accessor.wasNull());

    try (ResultSet resultSet = array.getResultSet()) {
      Assert.assertTrue(resultSet.next());
      Map<?, ?> entry = resultSet.getObject(1, Map.class);
      Assert.assertEquals(1, entry.get("key"));
      Assert.assertEquals(11, entry.get("value"));
      Assert.assertTrue(resultSet.next());
      entry = resultSet.getObject(1, Map.class);
      Assert.assertEquals(2, entry.get("key"));
      Assert.assertEquals(22, entry.get("value"));
      Assert.assertTrue(resultSet.next());
      entry = resultSet.getObject(1, Map.class);
      Assert.assertEquals(3, entry.get("key"));
      Assert.assertEquals(33, entry.get("value"));
      Assert.assertFalse(resultSet.next());
    }

    cursor.next();
    array = accessor.getArray();
    Assert.assertNotNull(array);
    Assert.assertFalse(accessor.wasNull());
    try (ResultSet resultSet = array.getResultSet()) {
      Assert.assertTrue(resultSet.next());
      Map<?, ?> entry = resultSet.getObject(1, Map.class);
      Assert.assertEquals(2, entry.get("key"));
      Assert.assertNull(entry.get("value"));
      Assert.assertFalse(resultSet.next());
    }

    cursor.next();
    array = accessor.getArray();
    Assert.assertNotNull(array);
    Assert.assertFalse(accessor.wasNull());
    try (ResultSet resultSet = array.getResultSet()) {
      Assert.assertTrue(resultSet.next());
      Map<?, ?> entry = resultSet.getObject(1, Map.class);
      Assert.assertEquals(0, entry.get("key"));
      Assert.assertEquals(2000, entry.get("value"));
      Assert.assertTrue(resultSet.next());
      entry = resultSet.getObject(1, Map.class);
      Assert.assertEquals(1, entry.get("key"));
      Assert.assertEquals(2001, entry.get("value"));
      Assert.assertTrue(resultSet.next());
      entry = resultSet.getObject(1, Map.class);
      Assert.assertEquals(2, entry.get("key"));
      Assert.assertEquals(2002, entry.get("value"));
      Assert.assertTrue(resultSet.next());
      entry = resultSet.getObject(1, Map.class);
      Assert.assertEquals(3, entry.get("key"));
      Assert.assertEquals(2003, entry.get("value"));
      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void testShouldGetArrayReturnNull() {
    vector.setNull(0);
    ((StructVector) vector.getDataVector()).setNull(0);

    ArrowFlightJdbcMapVectorAccessor accessor =
        new ArrowFlightJdbcMapVectorAccessor(vector, () -> 0, (boolean wasNull) -> {
        });

    Assert.assertNull(accessor.getArray());
    Assert.assertTrue(accessor.wasNull());
  }
}

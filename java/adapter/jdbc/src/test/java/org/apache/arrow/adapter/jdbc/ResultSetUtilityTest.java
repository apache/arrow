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

package org.apache.arrow.adapter.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Test;

/** Tests of the ResultSetUtility. */
public class ResultSetUtilityTest {
  @Test
  public void testZeroRowResultSet() throws Exception {
    for (boolean reuseVectorSchemaRoot : new boolean[]{false, true}) {
      try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
        ResultSet rs = ResultSetUtility.generateEmptyResultSet();
        JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(
            allocator, JdbcToArrowUtils.getUtcCalendar(), /* include metadata */ false)
            .setReuseVectorSchemaRoot(reuseVectorSchemaRoot)
            .build();

        ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(rs, config);
        assertTrue("Iterator on zero row ResultSet should haveNext() before use", iter.hasNext());
        VectorSchemaRoot root = iter.next();
        assertNotNull("VectorSchemaRoot from first next() result should never be null", root);
        assertEquals("VectorSchemaRoot from empty ResultSet should have zero rows", 0, root.getRowCount());
        assertFalse("hasNext() should return false on empty ResultSets after initial next() call", iter.hasNext());
      }
    }
  }

  @Test
  public void testBasicResultSet() throws Exception {
    try (ResultSetUtility.MockResultSet resultSet = ResultSetUtility.generateBasicResultSet(3)) {
      // Before row 1:
      assertTrue(resultSet.isBeforeFirst());
      assertFalse(resultSet.isFirst());
      assertFalse(resultSet.isLast());
      assertFalse(resultSet.isAfterLast());
      assertThrows(SQLException.class, () -> resultSet.getString(1));

      // Row 1:
      assertTrue(resultSet.next());
      assertFalse(resultSet.isBeforeFirst());
      assertTrue(resultSet.isFirst());
      assertFalse(resultSet.isLast());
      assertFalse(resultSet.isAfterLast());
      assertEquals("row number: 1", resultSet.getString(1));

      // Row 2:
      assertTrue(resultSet.next());
      assertFalse(resultSet.isBeforeFirst());
      assertFalse(resultSet.isFirst());
      assertFalse(resultSet.isLast());
      assertFalse(resultSet.isAfterLast());
      assertEquals("row number: 2", resultSet.getString(1));

      // Row 3:
      assertTrue(resultSet.next());
      assertFalse(resultSet.isBeforeFirst());
      assertFalse(resultSet.isFirst());
      assertTrue(resultSet.isLast());
      assertFalse(resultSet.isAfterLast());
      assertEquals("row number: 3", resultSet.getString(1));

      // After row 3:
      assertFalse(resultSet.next());
      assertFalse(resultSet.isBeforeFirst());
      assertFalse(resultSet.isFirst());
      assertFalse(resultSet.isLast());
      assertTrue(resultSet.isAfterLast());
    }
  }

  @Test
  public void testMockDataTypes() throws SQLException {
    ResultSetUtility.MockDataElement element = new ResultSetUtility.MockDataElement(1L, Types.NUMERIC);
    assertEquals(1L, element.getLong());
    assertEquals(1, element.getInt());
    assertEquals("1", element.getString());
  }
}

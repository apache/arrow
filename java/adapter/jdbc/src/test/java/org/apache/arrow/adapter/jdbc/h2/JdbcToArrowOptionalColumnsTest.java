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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * JUnit Test Class which contains methods to test JDBC to Arrow data conversion functionality for
 * (non-)optional columns, in particular with regard to the ensuing VectorSchemaRoot's schema.
 */
@RunWith(Parameterized.class)
public class JdbcToArrowOptionalColumnsTest extends AbstractJdbcToArrowTest {
  private static final String[] testFiles = {
    "h2/test1_null_and_notnull.yml"
  };

  /**
   * Constructor which populates the table object for each test iteration.
   *
   * @param table Table object
   */
  public JdbcToArrowOptionalColumnsTest(Table table) {
    this.table = table;
  }

  /**
   * Get the test data as a collection of Table objects for each test iteration.
   *
   * @return Collection of Table objects
   * @throws SQLException           on error
   * @throws ClassNotFoundException on error
   * @throws IOException            on error
   */
  @Parameterized.Parameters
  public static Collection<Object[]> getTestData() throws SQLException, ClassNotFoundException, IOException {
    return Arrays.asList(prepareTestData(testFiles, JdbcToArrowOptionalColumnsTest.class));
  }

  /**
   * Test Method to test JdbcToArrow Functionality for dealing with nullable and non-nullable columns.
   */
  @Test
  public void testJdbcToArrowValues() throws SQLException, IOException {
    testDataSets(sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE)));
  }

  /**
   * This method calls the assert methods for various DataSets. We verify that a SQL `NULL` column becomes
   * nullable in the VectorSchemaRoot, and that a SQL `NOT NULL` column becomes non-nullable.
   *
   * @param root VectorSchemaRoot for test
   */
  public void testDataSets(VectorSchemaRoot root) {
    JdbcToArrowTestHelper.assertFieldMetadataIsEmpty(root);

    assertTrue(root.getSchema().getFields().get(0).isNullable());
    assertFalse(root.getSchema().getFields().get(1).isNullable());
  }

}

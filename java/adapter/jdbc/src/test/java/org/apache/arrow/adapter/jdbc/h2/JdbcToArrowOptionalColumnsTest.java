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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * JUnit Test Class which contains methods to test JDBC to Arrow data conversion functionality for
 * (non-)optional columns, in particular with regard to the ensuing VectorSchemaRoot's schema.
 */
public class JdbcToArrowOptionalColumnsTest extends AbstractJdbcToArrowTest {
  private static final String[] testFiles = {"h2/test1_null_and_notnull.yml"};

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
    return Arrays.stream(prepareTestData(testFiles, JdbcToArrowOptionalColumnsTest.class))
        .map(Arguments::of);
  }

  /**
   * Test Method to test JdbcToArrow Functionality for dealing with nullable and non-nullable
   * columns.
   */
  @ParameterizedTest
  @MethodSource("getTestData")
  public void testJdbcToArrowValues(Table table)
      throws SQLException, IOException, ClassNotFoundException {
    this.initializeDatabase(table);

    testDataSets(sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE)), false);
  }

  /**
   * This method calls the assert methods for various DataSets. We verify that a SQL `NULL` column
   * becomes nullable in the VectorSchemaRoot, and that a SQL `NOT NULL` column becomes
   * non-nullable.
   *
   * @param root VectorSchemaRoot for test
   * @param isIncludeMapVector is this dataset checks includes map column. Jdbc type to 'map'
   *     mapping declared in configuration only manually
   */
  @Override
  public void testDataSets(VectorSchemaRoot root, boolean isIncludeMapVector) {
    JdbcToArrowTestHelper.assertFieldMetadataIsEmpty(root);

    assertTrue(root.getSchema().getFields().get(0).isNullable());
    assertFalse(root.getSchema().getFields().get(1).isNullable());
  }
}

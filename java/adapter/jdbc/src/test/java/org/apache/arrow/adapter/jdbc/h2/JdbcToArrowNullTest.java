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

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertNullValues;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;

import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * JUnit Test Class which contains methods to test JDBC to Arrow data conversion functionality with null values for
 * H2 database.
 */
@RunWith(Parameterized.class)
public class JdbcToArrowNullTest extends AbstractJdbcToArrowTest {

  private static final String NULL = "null";
  private static final String SELECTED_NULL_COLUMN = "selected_null_column";

  private static final String[] testFiles = {
    "h2/test1_all_datatypes_null_h2.yml",
    "h2/test1_selected_datatypes_null_h2.yml"
  };

  /**
   * Constructor which populate table object for each test iteration.
   *
   * @param table Table object
   */
  public JdbcToArrowNullTest(Table table) {
    this.table = table;
  }

  /**
   * Get the test data as a collection of Table objects for each test iteration.
   *
   * @return Collection of Table objects
   * @throws SQLException on error
   * @throws ClassNotFoundException on error
   * @throws IOException on error
   */
  @Parameters
  public static Collection<Object[]> getTestData() throws SQLException, ClassNotFoundException, IOException {
    return Arrays.asList(prepareTestData(testFiles, JdbcToArrowNullTest.class));
  }

  /**
   * Test Method to test JdbcToArrow Functionality for various H2 DB based datatypes with null values.
   */
  @Test
  public void testJdbcToArroValues() throws SQLException, IOException {
    testDataSets(JdbcToArrow.sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE),
        Calendar.getInstance()));
    testDataSets(JdbcToArrow.sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE)));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()),
        new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance()));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery())));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()),
        new RootAllocator(Integer.MAX_VALUE)));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()), Calendar.getInstance()));
  }


  /**
   * This method calls the assert methods for various DataSets.
   *
   * @param root VectorSchemaRoot for test
   */
  public void testDataSets(VectorSchemaRoot root) {
    switch (table.getType()) {
      case NULL:
        sqlToArrowTestNullValues(table.getVectors(), root, table.getRowCount());
        break;
      case SELECTED_NULL_COLUMN:
        sqlToArrowTestSelectedNullColumnsValues(table.getVectors(), root, table.getRowCount());
        break;
      default:
        // do nothing
        break;
    }
  }

  /**
   * This method assert tests null values in vectors for all the datatypes.
   *
   * @param vectors Vectors to test
   * @param root VectorSchemaRoot for test
   * @param rowCount number of rows
   */
  public void sqlToArrowTestNullValues(String[] vectors, VectorSchemaRoot root, int rowCount) {
    assertNullValues((IntVector) root.getVector(vectors[0]), rowCount);
    assertNullValues((BitVector) root.getVector(vectors[1]), rowCount);
    assertNullValues((TinyIntVector) root.getVector(vectors[2]), rowCount);
    assertNullValues((SmallIntVector) root.getVector(vectors[3]), rowCount);
    assertNullValues((BigIntVector) root.getVector(vectors[4]), rowCount);
    assertNullValues((DecimalVector) root.getVector(vectors[5]), rowCount);
    assertNullValues((Float8Vector) root.getVector(vectors[6]), rowCount);
    assertNullValues((Float4Vector) root.getVector(vectors[7]), rowCount);
    assertNullValues((TimeMilliVector) root.getVector(vectors[8]), rowCount);
    assertNullValues((DateMilliVector) root.getVector(vectors[9]), rowCount);
    assertNullValues((TimeStampVector) root.getVector(vectors[10]), rowCount);
    assertNullValues((VarBinaryVector) root.getVector(vectors[11]), rowCount);
    assertNullValues((VarCharVector) root.getVector(vectors[12]), rowCount);
    assertNullValues((VarBinaryVector) root.getVector(vectors[13]), rowCount);
    assertNullValues((VarCharVector) root.getVector(vectors[14]), rowCount);
    assertNullValues((VarCharVector) root.getVector(vectors[15]), rowCount);
    assertNullValues((BitVector) root.getVector(vectors[16]), rowCount);
  }

  /**
   * This method assert tests null values in vectors for some selected datatypes.
   *
   * @param vectors Vectors to test
   * @param root VectorSchemaRoot for test
   * @param rowCount number of rows
   */
  public void sqlToArrowTestSelectedNullColumnsValues(String[] vectors, VectorSchemaRoot root, int rowCount) {
    assertNullValues((BigIntVector) root.getVector(vectors[0]), rowCount);
    assertNullValues((DecimalVector) root.getVector(vectors[1]), rowCount);
    assertNullValues((Float8Vector) root.getVector(vectors[2]), rowCount);
    assertNullValues((Float4Vector) root.getVector(vectors[3]), rowCount);
    assertNullValues((TimeMilliVector) root.getVector(vectors[4]), rowCount);
    assertNullValues((DateMilliVector) root.getVector(vectors[5]), rowCount);
    assertNullValues((TimeStampVector) root.getVector(vectors[6]), rowCount);
    assertNullValues((VarBinaryVector) root.getVector(vectors[7]), rowCount);
    assertNullValues((VarCharVector) root.getVector(vectors[8]), rowCount);
    assertNullValues((VarBinaryVector) root.getVector(vectors[9]), rowCount);
    assertNullValues((VarCharVector) root.getVector(vectors[10]), rowCount);
    assertNullValues((VarCharVector) root.getVector(vectors[11]), rowCount);
    assertNullValues((BitVector) root.getVector(vectors[12]), rowCount);
  }

}

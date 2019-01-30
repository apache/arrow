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

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertDateVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTimeStampVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTimeVectorValues;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.TimeZone;

import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * JUnit Test Class which contains methods to test JDBC to Arrow data conversion functionality with TimeZone based Date,
 * Time and Timestamp datatypes for H2 database.
 */

@RunWith(Parameterized.class)
public class JdbcToArrowTimeZoneTest extends AbstractJdbcToArrowTest {

  private static final String EST_DATE = "est_date";
  private static final String EST_TIME = "est_time";
  private static final String EST_TIMESTAMP = "est_timestamp";
  private static final String GMT_DATE = "gmt_date";
  private static final String GMT_TIME = "gmt_time";
  private static final String GMT_TIMESTAMP = "gmt_timestamp";
  private static final String PST_DATE = "pst_date";
  private static final String PST_TIME = "pst_time";
  private static final String PST_TIMESTAMP = "pst_timestamp";

  private static final String[] testFiles = {
    "h2/test1_est_date_h2.yml",
    "h2/test1_est_time_h2.yml",
    "h2/test1_est_timestamp_h2.yml",
    "h2/test1_gmt_date_h2.yml",
    "h2/test1_gmt_time_h2.yml",
    "h2/test1_gmt_timestamp_h2.yml",
    "h2/test1_pst_date_h2.yml",
    "h2/test1_pst_time_h2.yml",
    "h2/test1_pst_timestamp_h2.yml"
  };

  /**
   * Constructor which populate table object for each test iteration.
   *
   * @param table Table object
   */
  public JdbcToArrowTimeZoneTest(Table table) {
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
    return Arrays.asList(prepareTestData(testFiles, JdbcToArrowTimeZoneTest.class));
  }

  /**
   * Test Method to test JdbcToArrow Functionality for various H2 DB based datatypes with TimeZone based Date,
   * Time and Timestamp datatype.
   */
  @Test
  public void testJdbcToArroValues() throws SQLException, IOException {
    testDataSets(JdbcToArrow.sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE),
        Calendar.getInstance(TimeZone.getTimeZone(table.getTimezone()))));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()),
        new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance(TimeZone.getTimeZone(table.getTimezone()))));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()),
        Calendar.getInstance(TimeZone.getTimeZone(table.getTimezone()))));
    testDataSets(JdbcToArrow.sqlToArrow(
        conn.createStatement().executeQuery(table.getQuery()),
        new JdbcToArrowConfigBuilder(
            new RootAllocator(Integer.MAX_VALUE),
            Calendar.getInstance(TimeZone.getTimeZone(table.getTimezone()))).build()));
    testDataSets(JdbcToArrow.sqlToArrow(
        conn,
        table.getQuery(),
        new JdbcToArrowConfigBuilder(
            new RootAllocator(Integer.MAX_VALUE),
            Calendar.getInstance(TimeZone.getTimeZone(table.getTimezone()))).build()));
  }

  /**
   * This method calls the assert methods for various DataSets.
   *
   * @param root VectorSchemaRoot for test
   */
  public void testDataSets(VectorSchemaRoot root) {
    switch (table.getType()) {
      case EST_DATE:
      case GMT_DATE:
      case PST_DATE:
        assertDateVectorValues((DateMilliVector) root.getVector(table.getVector()), table.getValues().length,
            table.getLongValues());
        break;
      case EST_TIME:
      case GMT_TIME:
      case PST_TIME:
        assertTimeVectorValues((TimeMilliVector) root.getVector(table.getVector()), table.getValues().length,
            table.getLongValues());
        break;
      case EST_TIMESTAMP:
      case GMT_TIMESTAMP:
      case PST_TIMESTAMP:
        assertTimeStampVectorValues((TimeStampVector) root.getVector(table.getVector()), table.getValues().length,
            table.getLongValues());
        break;
      default:
        // do nothing
        break;
    }
  }

}

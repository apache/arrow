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

package org.apache.arrow.driver.jdbc.test;

import static org.apache.arrow.driver.jdbc.utils.BaseProperty.HOST;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.PASSWORD;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.PORT;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.USERNAME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.utils.BaseProperty;
import org.hamcrest.CoreMatchers;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ResultSetMetadataTest {

  private static final Map<BaseProperty, Object> properties;
  private static ResultSetMetaData metadata;

  @Rule
  public ErrorCollector collector = new ErrorCollector();

  @ClassRule
  public static final FlightServerTestRule rule;

  static {
    properties = new HashMap<>();
    properties.put(HOST, "localhost");
    properties.put(PORT, (new Random()).nextInt(65536));
    properties.put(USERNAME, "flight-test-user");
    properties.put(PASSWORD, "flight-test-password");
    rule = new FlightServerTestRule(properties);
  }

  @BeforeClass
  public static void setup() throws SQLException {
    Properties properties = new Properties();
    properties.put(USERNAME.getEntry().getKey(), rule.getProperty(USERNAME));
    properties.put(PASSWORD.getEntry().getKey(), rule.getProperty(PASSWORD));

    try (Connection connection = (new ArrowFlightJdbcDriver())
        .connect("jdbc:arrow-flight://" +
                rule.getProperty(HOST) + ":" +
                rule.getProperty(PORT),
            properties)) {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery("SELECT * FROM METADATA");

      metadata = resultSet.getMetaData();
    }
  }

  /**
   * Test if {@link ResultSetMetaData} object is not null.
   */
  @Test
  public void testShouldGetResultSetMetadata() {
    collector.checkThat(metadata, CoreMatchers.is(notNullValue()));
  }

  /**
   * Test if {@link ResultSetMetaData#getColumnCount()} returns the correct values.
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testShouldGetColumnCount() throws SQLException {
    final int columnCount = metadata.getColumnCount();

    assert columnCount == 3;
  }
}

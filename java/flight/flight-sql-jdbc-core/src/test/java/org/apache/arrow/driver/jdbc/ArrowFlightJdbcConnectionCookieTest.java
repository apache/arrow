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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightJdbcConnectionCookieTest {

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE =
      FlightServerTestRule.createStandardTestRule(CoreMockedSqlProducers.getLegacyProducer());

  @Test
  public void testCookies() throws SQLException {
    try (Connection connection = FLIGHT_SERVER_TEST_RULE.getConnection(false);
         Statement statement = connection.createStatement()) {

      // Expect client didn't receive cookies before any operation
      Assert.assertNull(FLIGHT_SERVER_TEST_RULE.getMiddlewareCookieFactory().getCookie());

      // Run another action for check if the cookies was sent by the server.
      statement.execute(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD);
      Assert.assertEquals("k=v", FLIGHT_SERVER_TEST_RULE.getMiddlewareCookieFactory().getCookie());
    }
  }
}


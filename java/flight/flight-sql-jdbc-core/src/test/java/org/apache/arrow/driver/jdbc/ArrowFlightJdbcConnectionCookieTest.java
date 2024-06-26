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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ArrowFlightJdbcConnectionCookieTest {

  @RegisterExtension
  public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION =
      FlightServerTestExtension.createStandardTestExtension(
          CoreMockedSqlProducers.getLegacyProducer());

  @Test
  public void testCookies() throws SQLException {
    try (Connection connection = FLIGHT_SERVER_TEST_EXTENSION.getConnection(false);
        Statement statement = connection.createStatement()) {

      // Expect client didn't receive cookies before any operation
      assertNull(FLIGHT_SERVER_TEST_EXTENSION.getMiddlewareCookieFactory().getCookie());

      // Run another action for check if the cookies was sent by the server.
      statement.execute(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD);
      assertEquals("k=v", FLIGHT_SERVER_TEST_EXTENSION.getMiddlewareCookieFactory().getCookie());
    }
  }
}

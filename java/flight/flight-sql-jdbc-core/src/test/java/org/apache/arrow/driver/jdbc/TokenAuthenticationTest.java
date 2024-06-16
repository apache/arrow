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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.arrow.driver.jdbc.authentication.TokenAuthentication;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.util.AutoCloseables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TokenAuthenticationTest {
  private static final MockFlightSqlProducer FLIGHT_SQL_PRODUCER = new MockFlightSqlProducer();

  @RegisterExtension public static FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION;

  static {
    FLIGHT_SERVER_TEST_EXTENSION =
        new FlightServerTestExtension.Builder()
            .authentication(new TokenAuthentication.Builder().token("1234").build())
            .producer(FLIGHT_SQL_PRODUCER)
            .build();
  }

  @AfterAll
  public static void tearDownAfterClass() {
    AutoCloseables.closeNoChecked(FLIGHT_SQL_PRODUCER);
  }

  @Test
  public void connectUsingTokenAuthenticationShouldFail() throws SQLException {
    assertThrows(
        SQLException.class, () -> FLIGHT_SERVER_TEST_EXTENSION.getConnection(false, "invalid"));
  }

  @Test
  public void connectUsingTokenAuthenticationShouldSuccess() throws SQLException {
    try (Connection connection = FLIGHT_SERVER_TEST_EXTENSION.getConnection(false, "1234")) {
      assertFalse(connection.isClosed());
    }
  }
}

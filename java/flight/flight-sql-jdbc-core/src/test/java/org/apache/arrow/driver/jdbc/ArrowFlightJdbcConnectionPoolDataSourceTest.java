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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import javax.sql.PooledConnection;
import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.utils.ConnectionWrapper;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ArrowFlightJdbcConnectionPoolDataSourceTest {

  @RegisterExtension public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION;

  private static final MockFlightSqlProducer PRODUCER = new MockFlightSqlProducer();

  static {
    UserPasswordAuthentication authentication =
        new UserPasswordAuthentication.Builder()
            .user("user1", "pass1")
            .user("user2", "pass2")
            .build();

    FLIGHT_SERVER_TEST_EXTENSION =
        new FlightServerTestExtension.Builder()
            .authentication(authentication)
            .producer(PRODUCER)
            .build();
  }

  private ArrowFlightJdbcConnectionPoolDataSource dataSource;

  @BeforeEach
  public void setUp() {
    dataSource = FLIGHT_SERVER_TEST_EXTENSION.createConnectionPoolDataSource(false);
  }

  @AfterEach
  public void tearDown() throws Exception {
    dataSource.close();
  }

  @Test
  public void testShouldInnerConnectionIsClosedReturnCorrectly() throws Exception {
    PooledConnection pooledConnection = dataSource.getPooledConnection();
    Connection connection = pooledConnection.getConnection();
    assertFalse(connection.isClosed());
    connection.close();
    assertTrue(connection.isClosed());
  }

  @Test
  public void testShouldInnerConnectionShouldIgnoreDoubleClose() throws Exception {
    PooledConnection pooledConnection = dataSource.getPooledConnection();
    Connection connection = pooledConnection.getConnection();
    assertFalse(connection.isClosed());
    connection.close();
    assertTrue(connection.isClosed());
  }

  @Test
  public void testShouldInnerConnectionIsClosedReturnTrueIfPooledConnectionCloses()
      throws Exception {
    PooledConnection pooledConnection = dataSource.getPooledConnection();
    Connection connection = pooledConnection.getConnection();
    assertFalse(connection.isClosed());
    pooledConnection.close();
    assertTrue(connection.isClosed());
  }

  @Test
  public void testShouldReuseConnectionsOnPool() throws Exception {
    PooledConnection pooledConnection = dataSource.getPooledConnection("user1", "pass1");
    ConnectionWrapper connection = ((ConnectionWrapper) pooledConnection.getConnection());
    assertFalse(connection.isClosed());
    connection.close();
    assertTrue(connection.isClosed());
    assertFalse(connection.unwrap(ArrowFlightConnection.class).isClosed());

    PooledConnection pooledConnection2 = dataSource.getPooledConnection("user1", "pass1");
    ConnectionWrapper connection2 = ((ConnectionWrapper) pooledConnection2.getConnection());
    assertFalse(connection2.isClosed());
    connection2.close();
    assertTrue(connection2.isClosed());
    assertFalse(connection2.unwrap(ArrowFlightConnection.class).isClosed());

    assertSame(pooledConnection, pooledConnection2);
    assertNotSame(connection, connection2);
    assertSame(
        connection.unwrap(ArrowFlightConnection.class),
        connection2.unwrap(ArrowFlightConnection.class));
  }

  @Test
  public void testShouldNotMixConnectionsForDifferentUsers() throws Exception {
    PooledConnection pooledConnection = dataSource.getPooledConnection("user1", "pass1");
    ConnectionWrapper connection = ((ConnectionWrapper) pooledConnection.getConnection());
    assertFalse(connection.isClosed());
    connection.close();
    assertTrue(connection.isClosed());
    assertFalse(connection.unwrap(ArrowFlightConnection.class).isClosed());

    PooledConnection pooledConnection2 = dataSource.getPooledConnection("user2", "pass2");
    ConnectionWrapper connection2 = ((ConnectionWrapper) pooledConnection2.getConnection());
    assertFalse(connection2.isClosed());
    connection2.close();
    assertTrue(connection2.isClosed());
    assertFalse(connection2.unwrap(ArrowFlightConnection.class).isClosed());

    assertNotSame(pooledConnection, pooledConnection2);
    assertNotSame(connection, connection2);
    assertNotSame(
        connection.unwrap(ArrowFlightConnection.class),
        connection2.unwrap(ArrowFlightConnection.class));
  }
}

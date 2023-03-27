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

import javax.sql.PooledConnection;

import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.utils.ConnectionWrapper;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class ArrowFlightJdbcConnectionPoolDataSourceTest {
  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE;

  private static final MockFlightSqlProducer PRODUCER = new MockFlightSqlProducer();

  static {
    UserPasswordAuthentication authentication =
        new UserPasswordAuthentication.Builder()
            .user("user1", "pass1")
            .user("user2", "pass2")
            .build();

    FLIGHT_SERVER_TEST_RULE = new FlightServerTestRule.Builder()
        .authentication(authentication)
        .producer(PRODUCER)
        .build();
  }

  private ArrowFlightJdbcConnectionPoolDataSource dataSource;

  @Before
  public void setUp() {
    dataSource = FLIGHT_SERVER_TEST_RULE.createConnectionPoolDataSource(false);
  }

  @After
  public void tearDown() throws Exception {
    dataSource.close();
  }

  @Test
  public void testShouldInnerConnectionIsClosedReturnCorrectly() throws Exception {
    PooledConnection pooledConnection = dataSource.getPooledConnection();
    Connection connection = pooledConnection.getConnection();
    Assert.assertFalse(connection.isClosed());
    connection.close();
    Assert.assertTrue(connection.isClosed());
  }

  @Test
  public void testShouldInnerConnectionShouldIgnoreDoubleClose() throws Exception {
    PooledConnection pooledConnection = dataSource.getPooledConnection();
    Connection connection = pooledConnection.getConnection();
    Assert.assertFalse(connection.isClosed());
    connection.close();
    Assert.assertTrue(connection.isClosed());
  }

  @Test
  public void testShouldInnerConnectionIsClosedReturnTrueIfPooledConnectionCloses()
      throws Exception {
    PooledConnection pooledConnection = dataSource.getPooledConnection();
    Connection connection = pooledConnection.getConnection();
    Assert.assertFalse(connection.isClosed());
    pooledConnection.close();
    Assert.assertTrue(connection.isClosed());
  }

  @Test
  public void testShouldReuseConnectionsOnPool() throws Exception {
    PooledConnection pooledConnection = dataSource.getPooledConnection("user1", "pass1");
    ConnectionWrapper connection = ((ConnectionWrapper) pooledConnection.getConnection());
    Assert.assertFalse(connection.isClosed());
    connection.close();
    Assert.assertTrue(connection.isClosed());
    Assert.assertFalse(connection.unwrap(ArrowFlightConnection.class).isClosed());

    PooledConnection pooledConnection2 = dataSource.getPooledConnection("user1", "pass1");
    ConnectionWrapper connection2 = ((ConnectionWrapper) pooledConnection2.getConnection());
    Assert.assertFalse(connection2.isClosed());
    connection2.close();
    Assert.assertTrue(connection2.isClosed());
    Assert.assertFalse(connection2.unwrap(ArrowFlightConnection.class).isClosed());

    Assert.assertSame(pooledConnection, pooledConnection2);
    Assert.assertNotSame(connection, connection2);
    Assert.assertSame(connection.unwrap(ArrowFlightConnection.class),
        connection2.unwrap(ArrowFlightConnection.class));
  }

  @Test
  public void testShouldNotMixConnectionsForDifferentUsers() throws Exception {
    PooledConnection pooledConnection = dataSource.getPooledConnection("user1", "pass1");
    ConnectionWrapper connection = ((ConnectionWrapper) pooledConnection.getConnection());
    Assert.assertFalse(connection.isClosed());
    connection.close();
    Assert.assertTrue(connection.isClosed());
    Assert.assertFalse(connection.unwrap(ArrowFlightConnection.class).isClosed());

    PooledConnection pooledConnection2 = dataSource.getPooledConnection("user2", "pass2");
    ConnectionWrapper connection2 = ((ConnectionWrapper) pooledConnection2.getConnection());
    Assert.assertFalse(connection2.isClosed());
    connection2.close();
    Assert.assertTrue(connection2.isClosed());
    Assert.assertFalse(connection2.unwrap(ArrowFlightConnection.class).isClosed());

    Assert.assertNotSame(pooledConnection, pooledConnection2);
    Assert.assertNotSame(connection, connection2);
    Assert.assertNotSame(connection.unwrap(ArrowFlightConnection.class),
        connection2.unwrap(ArrowFlightConnection.class));
  }
}

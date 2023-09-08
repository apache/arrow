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

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Tests for {@link ArrowFlightJdbcDriver}.
 */
public class ArrowFlightJdbcFactoryTest {

  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE;
  private static final MockFlightSqlProducer PRODUCER = new MockFlightSqlProducer();

  static {
    UserPasswordAuthentication authentication =
        new UserPasswordAuthentication.Builder().user("user1", "pass1").user("user2", "pass2")
            .build();

    FLIGHT_SERVER_TEST_RULE = new FlightServerTestRule.Builder()
        .authentication(authentication)
        .producer(PRODUCER)
        .build();
  }

  private BufferAllocator allocator;
  private ArrowFlightJdbcConnectionPoolDataSource dataSource;

  @Before
  public void setUp() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
    dataSource = FLIGHT_SERVER_TEST_RULE.createConnectionPoolDataSource();
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(dataSource, allocator);
  }

  @Test
  public void testShouldBeAbleToEstablishAConnectionSuccessfully() throws Exception {
    UnregisteredDriver driver = new ArrowFlightJdbcDriver();
    Constructor<ArrowFlightJdbcFactory> constructor = ArrowFlightJdbcFactory.class.getConstructor();
    constructor.setAccessible(true);
    ArrowFlightJdbcFactory factory = constructor.newInstance();

    final Properties properties = new Properties();
    properties.putAll(ImmutableMap.of(
        ArrowFlightConnectionProperty.HOST.camelName(), "localhost",
        ArrowFlightConnectionProperty.PORT.camelName(), 32010,
        ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), false));

    try (Connection connection = factory.newConnection(driver, constructor.newInstance(),
        "jdbc:arrow-flight-sql://localhost:32010", properties)) {
      assert connection.isValid(300);
    }
  }
}

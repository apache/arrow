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

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcConnectionPoolDataSource;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDataSource;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.ConnectionProperty;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.alexpanov.net.FreePortFinder;

/**
 * Utility class for unit tests that need to instantiate a {@link FlightServer}
 * and interact with it.
 */
public class FlightServerTestRule implements TestRule, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlightServerTestRule.class);

  private final Properties properties;
  private final ArrowFlightConnectionConfigImpl config;
  private final BufferAllocator allocator;
  private final FlightSqlProducer producer;

  private final Map<String, String> validCredentials = new HashMap<>();

  private FlightServerTestRule(final Properties properties,
                               final ArrowFlightConnectionConfigImpl config,
                               final BufferAllocator allocator,
                               final FlightSqlProducer producer) {
    this.properties = Preconditions.checkNotNull(properties);
    this.config = Preconditions.checkNotNull(config);
    this.allocator = Preconditions.checkNotNull(allocator);
    this.producer = Preconditions.checkNotNull(producer);
  }

  /**
   * Creates a new {@link FlightServerTestRule} for tests.
   *
   * @return a new test rule.
   */
  public static FlightServerTestRule createNewTestRule(final FlightSqlProducer producer) {
    final Map<ConnectionProperty, Object> configs = new HashMap<>();
    configs.put(ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.HOST, "localhost");
    configs.put(ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.PORT, FreePortFinder.findFreeLocalPort());
    configs.put(ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.USER, "flight-test-user");
    configs.put(ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.PASSWORD, "flight-test-password");

    final Properties properties = new Properties();
    configs.forEach((key, value) -> properties.put(key.camelName(), value == null ? key.defaultValue() : value));
    final FlightServerTestRule rule = new FlightServerTestRule(
        properties, new ArrowFlightConnectionConfigImpl(properties), new RootAllocator(Long.MAX_VALUE), producer);
    rule.validCredentials.put(
        properties.getProperty(ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.USER.camelName()),
        properties.getProperty(ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.PASSWORD.camelName()));
    return rule;
  }

  public void addUser(final String username, final String password) {
    validCredentials.put(username, password);
  }

  private boolean validateUser(final String username, final String password) {
    return validateUser(username) && validCredentials.get(username).equals(password);
  }

  private boolean validateUser(final String username) {
    return validCredentials.containsKey(username);
  }

  ArrowFlightJdbcDataSource createDataSource() {
    return ArrowFlightJdbcDataSource.createNewDataSource(properties);
  }

  public ArrowFlightJdbcConnectionPoolDataSource createConnectionPoolDataSource() {
    return ArrowFlightJdbcConnectionPoolDataSource.createNewDataSource(properties);
  }

  public Connection getConnection() throws SQLException {
    return this.createDataSource().getConnection();
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try (FlightServer flightServer =
                 getStartServer(location ->
                     FlightServer.builder(allocator, location, producer)
                         .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
                             new BasicCallHeaderAuthenticator(FlightServerTestRule.this::validate)))
                         .build(), 3)) {
          LOGGER.info("Started " + FlightServer.class.getName() + " as " + flightServer);
          base.evaluate();
        } finally {
          close();
        }
      }
    };
  }

  private FlightServer getStartServer(Function<Location, FlightServer> newServerFromLocation, int retries)
      throws IOException {

    final Deque<ReflectiveOperationException> exceptions = new ArrayDeque<>();

    for (; retries > 0; retries--) {
      final Location location = Location.forGrpcInsecure(config.getHost(), config.getPort());
      final FlightServer server = newServerFromLocation.apply(location);
      try {
        Method start = server.getClass().getMethod("start");
        start.setAccessible(true);
        start.invoke(server);
        return server;
      } catch (ReflectiveOperationException e) {
        exceptions.add(e);
      }
    }

    exceptions.forEach(e -> LOGGER.error("Failed to start a new " + FlightServer.class.getName() + ".", e));
    throw new IOException(exceptions.pop().getCause());
  }

  private CallHeaderAuthenticator.AuthResult validate(final String username,
                                                      final String password) {
    if (validateUser(username, password)) {
      return () -> username;
    }

    throw CallStatus.UNAUTHENTICATED.withDescription("Invalid credentials.").toRuntimeException();
  }

  @Override
  public void close() throws Exception {
    allocator.getChildAllocators().forEach(BufferAllocator::close);
    AutoCloseables.close(allocator);
  }
}

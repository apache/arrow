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

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;
import java.util.function.Function;

import org.apache.arrow.driver.jdbc.authentication.Authentication;
import org.apache.arrow.driver.jdbc.authentication.TokenAuthentication;
import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
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
  private FlightSqlProducer producer;
  private final Authentication authentication;

  private final MiddlewareCookie.Factory factory = new MiddlewareCookie.Factory();

  private FlightServerTestRule(final Properties properties,
                               final ArrowFlightConnectionConfigImpl config,
                               final BufferAllocator allocator,
                               final FlightSqlProducer producer,
                               final Authentication authentication) {
    this.properties = Preconditions.checkNotNull(properties);
    this.config = Preconditions.checkNotNull(config);
    this.allocator = Preconditions.checkNotNull(allocator);
    this.producer = Preconditions.checkNotNull(producer);
    this.authentication = authentication;
  }

  /**
   * Create a {@link FlightServerTestRule} with standard values such as: user, password, localhost.
   *
   * @param producer the producer used to create the FlightServerTestRule.
   * @return the FlightServerTestRule.
   */
  public static FlightServerTestRule createStandardTestRule(final FlightSqlProducer producer) {
    UserPasswordAuthentication authentication =
        new UserPasswordAuthentication.Builder()
            .user("flight-test-user", "flight-test-password")
            .build();

    return new FlightServerTestRule.Builder()
        .host("localhost")
        .randomPort()
        .authentication(authentication)
        .producer(producer)
        .build();
  }

  ArrowFlightJdbcDataSource createDataSource() {
    return ArrowFlightJdbcDataSource.createNewDataSource(properties);
  }

  ArrowFlightJdbcDataSource createDataSource(String token) {
    properties.put("token", token);
    return ArrowFlightJdbcDataSource.createNewDataSource(properties);
  }

  public ArrowFlightJdbcConnectionPoolDataSource createConnectionPoolDataSource() {
    return ArrowFlightJdbcConnectionPoolDataSource.createNewDataSource(properties);
  }

  public Connection getConnectionFromToken(String token) throws SQLException {
    return this.createDataSource(token).getConnection();
  }

  public Connection getConnection() throws SQLException {
    return this.createDataSource().getConnection();
  }

  public MiddlewareCookie.Factory getFactory() {
    return factory;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try (FlightServer flightServer =
                 getStartServer(location ->
                     FlightServer.builder(allocator, location, producer)
                         .headerAuthenticator(authentication.authenticate())
                         .middleware(FlightServerMiddleware.Key.of("KEY"), factory)
                         .build(), 3)) {
          LOGGER.info("Started " + FlightServer.class.getName() + " as " + flightServer);
          base.evaluate();
        } finally {
          close();
        }
      }
    };
  }

  private FlightServer getStartServer(Function<Location, FlightServer> newServerFromLocation,
                                      int retries)
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

    exceptions.forEach(
        e -> LOGGER.error("Failed to start a new " + FlightServer.class.getName() + ".", e));
    throw new IOException(exceptions.pop().getCause());
  }

  @Override
  public void close() throws Exception {
    allocator.getChildAllocators().forEach(BufferAllocator::close);
    AutoCloseables.close(allocator);
  }

  /**
   * Builder for {@link FlightServerTestRule}.
   */
  public static final class Builder {
    private final Properties properties = new Properties();
    private FlightSqlProducer producer;
    private Authentication authentication;

    /**
     * Sets the host for the server rule.
     *
     * @param host the host value.
     * @return the Builder.
     */
    public Builder host(final String host) {
      properties.put(ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.HOST.camelName(), host);
      return this;
    }

    /**
     * Sets a random port to be used by the server rule.
     *
     * @return the Builder.
     */
    public Builder randomPort() {
      properties.put(ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.PORT.camelName(),
          FreePortFinder.findFreeLocalPort());
      return this;
    }

    /**
     * Sets a specific port to be used by the server rule.
     *
     * @param port the port value.
     * @return the Builder.
     */
    public Builder port(final int port) {
      properties.put(ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.PORT.camelName(), port);
      return this;
    }

    /**
     * Sets the producer that will be used in the server rule.
     *
     * @param producer the flight sql producer.
     * @return the Builder.
     */
    public Builder producer(final FlightSqlProducer producer) {
      this.producer = producer;
      return this;
    }

    /**
     * Sets the type of the authentication that will be used in the server rules.
     * There are two types of authentication: {@link UserPasswordAuthentication} and
     * {@link TokenAuthentication}.
     *
     * @param authentication the type of authentication.
     * @return the Builder.
     */
    public Builder authentication(final Authentication authentication) {
      this.authentication = authentication;
      return this;
    }

    /**
     * Builds the {@link FlightServerTestRule} using the provided values.
     *
     * @return a {@link FlightServerTestRule}.
     */
    public FlightServerTestRule build() {
      authentication.populateProperties(properties);

      return new FlightServerTestRule(properties, new ArrowFlightConnectionConfigImpl(properties),
          new RootAllocator(Long.MAX_VALUE), producer, authentication);
    }
  }

  /**
   * A middleware to handle with the cookies in the server. It is used to test if cookies are
   * being sent properly.
   */
  static class MiddlewareCookie implements FlightServerMiddleware {

    private final Factory factory;

    public MiddlewareCookie(Factory factory) {
      this.factory = factory;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders callHeaders) {
      if (!factory.receivedCookieHeader) {
        callHeaders.insert("Set-Cookie", "k=v");
      }
    }

    @Override
    public void onCallCompleted(CallStatus callStatus) {

    }

    @Override
    public void onCallErrored(Throwable throwable) {

    }

    /**
     * A factory for the MiddlewareCookkie.
     */
    static class Factory implements FlightServerMiddleware.Factory<MiddlewareCookie> {

      private boolean receivedCookieHeader = false;
      private String cookie;

      @Override
      public MiddlewareCookie onCallStarted(CallInfo callInfo, CallHeaders callHeaders, RequestContext requestContext) {
        cookie = callHeaders.get("Cookie");
        receivedCookieHeader = null != cookie;
        return new MiddlewareCookie(this);
      }

      public String getCookie() {
        return cookie;
      }
    }
  }

}

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

import static org.apache.arrow.driver.jdbc.utils.FlightSqlTestCertificates.CertKeyPair;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;
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
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for unit tests that need to instantiate a {@link FlightServer} and interact with
 * it.
 */
public class FlightServerTestExtension
    implements BeforeAllCallback, AfterAllCallback, AutoCloseable {
  public static final String DEFAULT_USER = "flight-test-user";
  public static final String DEFAULT_PASSWORD = "flight-test-password";

  private static final Logger LOGGER = LoggerFactory.getLogger(FlightServerTestExtension.class);

  private final Properties properties;
  private final ArrowFlightConnectionConfigImpl config;
  private final BufferAllocator allocator;
  private final FlightSqlProducer producer;
  private final Authentication authentication;
  private final CertKeyPair certKeyPair;
  private final File mTlsCACert;

  private final MiddlewareCookie.Factory middlewareCookieFactory = new MiddlewareCookie.Factory();

  private FlightServerTestExtension(
      final Properties properties,
      final ArrowFlightConnectionConfigImpl config,
      final BufferAllocator allocator,
      final FlightSqlProducer producer,
      final Authentication authentication,
      final CertKeyPair certKeyPair,
      final File mTlsCACert) {
    this.properties = Preconditions.checkNotNull(properties);
    this.config = Preconditions.checkNotNull(config);
    this.allocator = Preconditions.checkNotNull(allocator);
    this.producer = Preconditions.checkNotNull(producer);
    this.authentication = authentication;
    this.certKeyPair = certKeyPair;
    this.mTlsCACert = mTlsCACert;
  }

  /**
   * Create a {@link FlightServerTestExtension} with standard values such as: user, password,
   * localhost.
   *
   * @param producer the producer used to create the FlightServerTestExtension.
   * @return the FlightServerTestExtension.
   */
  public static FlightServerTestExtension createStandardTestExtension(
      final FlightSqlProducer producer) {
    UserPasswordAuthentication authentication =
        new UserPasswordAuthentication.Builder().user(DEFAULT_USER, DEFAULT_PASSWORD).build();

    return new Builder().authentication(authentication).producer(producer).build();
  }

  ArrowFlightJdbcDataSource createDataSource() {
    return ArrowFlightJdbcDataSource.createNewDataSource(properties);
  }

  public ArrowFlightJdbcConnectionPoolDataSource createConnectionPoolDataSource() {
    return ArrowFlightJdbcConnectionPoolDataSource.createNewDataSource(properties);
  }

  public ArrowFlightJdbcConnectionPoolDataSource createConnectionPoolDataSource(
      boolean useEncryption) {
    setUseEncryption(useEncryption);
    return ArrowFlightJdbcConnectionPoolDataSource.createNewDataSource(properties);
  }

  public Connection getConnection(boolean useEncryption, String token) throws SQLException {
    properties.put("token", token);

    return getConnection(useEncryption);
  }

  public Connection getConnection(boolean useEncryption) throws SQLException {
    setUseEncryption(useEncryption);
    return this.createDataSource().getConnection();
  }

  private void setUseEncryption(boolean useEncryption) {
    properties.put("useEncryption", useEncryption);
  }

  public MiddlewareCookie.Factory getMiddlewareCookieFactory() {
    return middlewareCookieFactory;
  }

  @FunctionalInterface
  public interface CheckedFunction<T, R> {
    R apply(T t) throws IOException;
  }

  private FlightServer initiateServer(Location location) throws IOException {
    FlightServer.Builder builder =
        FlightServer.builder(allocator, location, producer)
            .headerAuthenticator(authentication.authenticate())
            .middleware(FlightServerMiddleware.Key.of("KEY"), middlewareCookieFactory);
    if (certKeyPair != null) {
      builder.useTls(certKeyPair.cert, certKeyPair.key);
    }
    if (mTlsCACert != null) {
      builder.useMTlsClientVerification(mTlsCACert);
    }
    return builder.build();
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    try {
      FlightServer flightServer = getStartServer(this::initiateServer, 3);
      properties.put("port", flightServer.getPort());
      LOGGER.info("Started " + FlightServer.class.getName() + " as " + flightServer);
      context.getStore(ExtensionContext.Namespace.GLOBAL).put("flightServer", flightServer);
    } catch (Exception e) {
      LOGGER.error("Failed to start FlightServer", e);
      throw e;
    }
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    FlightServer flightServer =
        context.getStore(ExtensionContext.Namespace.GLOBAL).get("flightServer", FlightServer.class);
    if (flightServer != null) {
      flightServer.close();
    }
    close();
  }

  private FlightServer getStartServer(
      CheckedFunction<Location, FlightServer> newServerFromLocation, int retries)
      throws IOException {
    final Deque<ReflectiveOperationException> exceptions = new ArrayDeque<>();
    for (; retries > 0; retries--) {
      final FlightServer server =
          newServerFromLocation.apply(Location.forGrpcInsecure("localhost", 0));
      try {
        Method start = server.getClass().getMethod("start");
        start.setAccessible(true);
        start.invoke(server);
        return server;
      } catch (ReflectiveOperationException e) {
        exceptions.add(e);
      }
    }
    exceptions.forEach(e -> LOGGER.error("Failed to start FlightServer", e));
    throw new IOException(exceptions.pop().getCause());
  }

  /**
   * Sets a port to be used.
   *
   * @return the port value.
   */
  public int getPort() {
    return config.getPort();
  }

  /**
   * Sets a host to be used.
   *
   * @return the host value.
   */
  public String getHost() {
    return config.getHost();
  }

  @Override
  public void close() throws Exception {
    allocator.getChildAllocators().forEach(BufferAllocator::close);
    AutoCloseables.close(allocator);
  }

  /** Builder for {@link FlightServerTestExtension}. */
  public static final class Builder {
    private final Properties properties;
    private FlightSqlProducer producer;
    private Authentication authentication;
    private CertKeyPair certKeyPair;
    private File mTlsCACert;

    public Builder() {
      this.properties = new Properties();
      this.properties.put("host", "localhost");
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
     * Sets the type of the authentication that will be used in the server rules. There are two
     * types of authentication: {@link UserPasswordAuthentication} and {@link TokenAuthentication}.
     *
     * @param authentication the type of authentication.
     * @return the Builder.
     */
    public Builder authentication(final Authentication authentication) {
      this.authentication = authentication;
      return this;
    }

    /**
     * Enable TLS on the server.
     *
     * @param certChain The certificate chain to use.
     * @param key The private key to use.
     * @return the Builder.
     */
    public Builder useEncryption(final File certChain, final File key) {
      certKeyPair = new CertKeyPair(certChain, key);
      return this;
    }

    /**
     * Enable Client Verification via mTLS on the server.
     *
     * @param mTlsCACert The CA certificate to use for client verification.
     * @return the Builder.
     */
    public Builder useMTlsClientVerification(final File mTlsCACert) {
      this.mTlsCACert = mTlsCACert;
      return this;
    }

    /**
     * Builds the {@link FlightServerTestExtension} using the provided values.
     *
     * @return a {@link FlightServerTestExtension}.
     */
    public FlightServerTestExtension build() {
      authentication.populateProperties(properties);
      return new FlightServerTestExtension(
          properties,
          new ArrowFlightConnectionConfigImpl(properties),
          new RootAllocator(Long.MAX_VALUE),
          producer,
          authentication,
          certKeyPair,
          mTlsCACert);
    }
  }

  /**
   * A middleware to handle with the cookies in the server. It is used to test if cookies are being
   * sent properly.
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
    public void onCallCompleted(CallStatus callStatus) {}

    @Override
    public void onCallErrored(Throwable throwable) {}

    /** A factory for the MiddlewareCookie. */
    static class Factory implements FlightServerMiddleware.Factory<MiddlewareCookie> {

      private boolean receivedCookieHeader = false;
      private String cookie;

      @Override
      public MiddlewareCookie onCallStarted(
          CallInfo callInfo, CallHeaders callHeaders, RequestContext requestContext) {
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

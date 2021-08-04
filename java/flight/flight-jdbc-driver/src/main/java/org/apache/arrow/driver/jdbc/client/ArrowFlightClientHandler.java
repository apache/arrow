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

package org.apache.arrow.driver.jdbc.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.arrow.driver.jdbc.client.utils.ClientAuthenticationUtils;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;

/**
 * An adhoc {@link FlightClient} wrapper, used to access the client. Allows for
 * the reuse of credentials and properties.
 */
public class ArrowFlightClientHandler implements FlightClientHandler {

  private final Deque<AutoCloseable> resources =
      new ArrayDeque<>();
  private final FlightClient client;

  @Nullable
  private CredentialCallOption token;

  @Nullable
  private HeaderCallOption properties;

  protected ArrowFlightClientHandler(final FlightClient client,
                                     @Nullable final CredentialCallOption token,
                                     @Nullable final HeaderCallOption properties) {
    this(client, token);
    this.properties = properties;
  }

  protected ArrowFlightClientHandler(final FlightClient client,
                                     @Nullable final CredentialCallOption token) {
    this(client);
    this.token = token;
  }

  protected ArrowFlightClientHandler(final FlightClient client,
                                     final HeaderCallOption properties) {
    this(client, null, properties);
  }

  protected ArrowFlightClientHandler(final FlightClient client) {
    this.client = client;
    this.resources.add(this.client);
  }

  /**
   * Gets the {@link FlightClient} wrapped by this handler.
   *
   * @return the client wrapped by this.
   */
  protected final FlightClient getClient() {
    return client;
  }

  /**
   * Gets the bearer token for subsequent calls to this client.
   *
   * @return the bearer token, if it exists; otherwise, empty.
   */
  protected final Optional<CredentialCallOption> getBearerToken() {
    return Optional.ofNullable(token);
  }

  /**
   * Gets the headers for subsequent calls to this client.
   *
   * @return the {@link #properties} of this client, if they exist; otherwise, empty.
   */
  protected final Optional<HeaderCallOption> getProperties() {
    return Optional.ofNullable(properties);
  }

  /**
   * Makes an RPC "getInfo" request with the given query and client properties
   * in order to retrieve the metadata associated with a set of data records.
   *
   * @param query The query to retrieve FlightInfo for.
   * @return a {@link FlightInfo} object.
   */
  protected FlightInfo getInfo(final String query) {
    return client.getInfo(FlightDescriptor.command(query.getBytes(StandardCharsets.UTF_8)),
        token);
  }

  @Override
  public List<FlightStream> getFlightStreams(final String query) {
    final FlightInfo flightInfo = getInfo(query);
    final List<FlightEndpoint> endpoints = flightInfo.getEndpoints();

    final List<FlightStream> streams =
        endpoints.stream().map(flightEndpoint -> client.getStream(flightEndpoint.getTicket(), token))
            .collect(Collectors.toList());
    streams.forEach(resources::addFirst);

    return streams;
  }

  @Override
  public final void close() throws Exception {
    try {
      AutoCloseables.close(resources);
    } catch (final Exception e) {
      throw new IOException("Failed to close resources.", e);
    }
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param allocator    The {@link BufferAllocator}.
   * @param host         The host to connect to.
   * @param port         The port to connect to.
   * @param username     The username for authentication, if needed.
   * @param password     The password for authentication, if needed.
   * @param properties   The {@link HeaderCallOption} of this client, if needed.
   * @param keyStorePath The keystore path for establishing a TLS-encrypted connection, if
   *                     needed.
   * @param keyStorePass The keystore password for establishing a TLS-encrypted connection,
   *                     if needed.
   * @return a new {@link ArrowFlightClientHandler} based upon the aforementioned information.
   * @throws GeneralSecurityException If a certificate-related error occurs.
   * @throws IOException              If an error occurs while trying to establish a connection to the
   *                                  client.
   */
  public static final ArrowFlightClientHandler getClient(
      final BufferAllocator allocator, final String host, final int port,
      @Nullable final String username, @Nullable final String password,
      @Nullable final HeaderCallOption properties,
      final boolean useTls,
      @Nullable final String keyStorePath, @Nullable final String keyStorePass)
      throws GeneralSecurityException, IOException {

    /*
     * TODO Too many if/else clauses: REDUCE somehow.
     *
     * Do NOT resort to creating labels and breaking from them! A better
     * alternative would be splitting this method into smaller ones.
     */
    final FlightClient.Builder builder = FlightClient.builder()
        .allocator(allocator);

    ArrowFlightClientHandler handler;

    if (useTls || keyStorePath != null) {
      // Build a secure TLS-encrypted connection.
      builder.location(Location.forGrpcTls(host, port)).useTls();
    } else {
      // Build an insecure, basic connection.
      builder.location(Location.forGrpcInsecure(host, port));
    }

    if (keyStorePath != null) {
      final InputStream certificateStream = ClientAuthenticationUtils.getCertificateStream(keyStorePath, keyStorePass);
      builder.trustedCertificates(certificateStream);
    }

    /*
     * Check whether to use username/password credentials to authenticate to the
     * Flight Client.
     */
    final boolean useAuthentication = username != null;
    final FlightClient client;

    if (!useAuthentication) {
      client = builder.build();
      // Build an unauthenticated client.
      handler = new ArrowFlightClientHandler(client, properties);
    } else {
      final ClientIncomingAuthHeaderMiddleware.Factory factory = new ClientIncomingAuthHeaderMiddleware.Factory(
          new ClientBearerHeaderHandler());

      builder.intercept(factory);

      client = builder.build();

      // Build an authenticated client.
      handler = new ArrowFlightClientHandler(client, ClientAuthenticationUtils
          .getAuthenticate(client, username, password, factory, properties),
          properties);
    }

    handler.resources.addLast(client);
    return handler;
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param allocator  The {@link BufferAllocator}.
   * @param host       The host to connect to.
   * @param port       The port to connect to.
   * @param username   The username for authentication, if needed.
   * @param password   The password for authentication, if needed.
   * @param properties The {@link HeaderCallOption} of this client, if needed.
   * @return a new {@link ArrowFlightClientHandler} based upon the aforementioned information.
   * @throws GeneralSecurityException If a certificate-related error occurs.
   * @throws IOException              If an error occurs while trying to establish a connection to the
   *                                  client.
   */
  public static final ArrowFlightClientHandler getClient(
      final BufferAllocator allocator, final String host, final int port,
      @Nullable final String username, @Nullable final String password,
      @Nullable final HeaderCallOption properties)
      throws GeneralSecurityException, IOException {

    return getClient(allocator, host, port, username, password, properties,
        false, null, null);
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param allocator The {@link BufferAllocator}.
   * @param host      The host to connect to.
   * @param port      The port to connect to.
   * @param username  The username for authentication, if needed.
   * @param password  The password for authentication, if needed.
   * @return a new {@link ArrowFlightClientHandler} based upon the aforementioned information.
   * @throws GeneralSecurityException If a certificate-related error occurs.
   * @throws IOException              If an error occurs while trying to establish a connection to the
   *                                  client.
   */
  public static final ArrowFlightClientHandler getClient(
      final BufferAllocator allocator, final String host, final int port,
      @Nullable final String username, @Nullable final String password)
      throws GeneralSecurityException, IOException {

    return getClient(allocator, host, port, username, password, null);
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param allocator The {@link BufferAllocator}.
   * @param host      The host to connect to.
   * @param port      The port to connect to.
   * @return a new {@link ArrowFlightClientHandler} based upon the aforementioned information.
   * @throws GeneralSecurityException If a certificate-related error occurs.
   * @throws IOException              If an error occurs while trying to establish a connection to the
   *                                  client.
   */
  public static final ArrowFlightClientHandler getClient(
      final BufferAllocator allocator, final String host, final int port)
      throws GeneralSecurityException, IOException {

    return getClient(allocator, host, port, null, null);
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param allocator    The {@link BufferAllocator}.
   * @param host         The host to connect to.
   * @param port         The port to connect to.
   * @param properties   The {@link HeaderCallOption} of this client, if needed.
   * @param keyStorePath The keystore path for establishing a TLS-encrypted connection, if
   *                     needed.
   * @param keyStorePass The keystore password for establishing a TLS-encrypted connection,
   *                     if needed.
   * @return a new {@link ArrowFlightClientHandler} based upon the aforementioned information.
   * @throws GeneralSecurityException If a certificate-related error occurs.
   * @throws IOException              If an error occurs while trying to establish a connection to the client.
   */
  public static final ArrowFlightClientHandler getClient(
      final BufferAllocator allocator, final String host, final int port,
      @Nullable final HeaderCallOption properties,
      @Nullable final String keyStorePath, @Nullable final String keyStorePass)
      throws GeneralSecurityException, IOException {

    return getClient(allocator, host, port, null, null, properties, true, keyStorePath, keyStorePass);
  }
}

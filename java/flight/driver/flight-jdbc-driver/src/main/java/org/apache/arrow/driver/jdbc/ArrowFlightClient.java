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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

/**
 * An adhoc {@link FlightClient} wrapper used to access the client. Allows for
 * the reuse of credentials.
 */
public final class ArrowFlightClient implements AutoCloseable {

  private final FlightClient client;

  private final CredentialCallOption bearerToken;

  private ArrowFlightClient(FlightClient client,
      CredentialCallOption properties) {
    this.client = client;
    this.bearerToken = properties;
  }

  /**
   * Gets the Arrow Flight Client.
   *
   * @return the {@link FlightClient} wrapped by this.
   */
  protected FlightClient getClient() {
    return client;
  }

  /**
   * Gets the bearer token for the client wrapped by this.
   *
   * @return the {@link CredentialCallOption} of this client.
   */
  protected CredentialCallOption getProperties() {
    return bearerToken;
  }

  /**
   * Makes RPC requests to the Dremio Flight Server Endpoint to retrieve results
   * of the provided SQL query.
   *
   * @param query
   *          The SQL query to execute.
   * @param headerCallOption
   *          The client properties to execute provided SQL query with.
   * @throws Exception
   *           If an error occurs during query execution.
   */
  public VectorSchemaRoot runQuery(String query,
      HeaderCallOption headerCallOption) throws Exception {
    /*
     * TODO Run a query and return its corresponding VectorSchemaRoot, which
     * must later be converted into a ResultSet.
     */
    return null;
  }

  /**
   * Makes an RPC "getInfo" request with the given query and client properties
   * in order to retrieve the metadata associated with a set of data records.
   *
   * @param query
   *          The query to retrieve FlightInfo for.
   * @param options
   *          The client properties to execute this request with.
   * @return a {@link FlightInfo} object.
   */
  public FlightInfo getInfo(String query, CallOption... options) {
    return client.getInfo(
        FlightDescriptor.command(query.getBytes(StandardCharsets.UTF_8)),
        options);
  }

  /**
   * Makes an RPC "getStream" request based on the provided {@link FlightInfo}
   * object. Retrieves result of the query previously prepared with "getInfo."
   *
   * @param flightInfo
   *          The {@code FlightInfo} object encapsulating information for the
   *          server to identify the prepared statement with.
   * @param options
   *          The client properties to execute this request with.
   * @return a {@code FlightStream} of results.
   */
  public FlightStream getStream(FlightInfo flightInfo, CallOption... options) {
    return client.getStream(null, options);
  }

  /**
   * Creates a {@code ArrowFlightClient} wrapping a {@link FlightClient}
   * connected to the Dremio server without any encryption.
   *
   * @param allocator
   *          The buffer allocator to use for the {@code FlightClient} wrapped
   *          by this.
   * @param host
   *          The host to connect to.
   * @param port
   *          The port to connect to.
   * @param username
   *          The username to connect with.
   * @param password
   *          The password to connect with.
   * @param clientProperties
   *          The client properties to set during authentication.
   * @return a new {@code ArrowFlightClient} wrapping a non-encrypted
   *         {@code FlightClient}, with a bearer token for subsequent requests
   *         to the wrapped client.
   */
  public static ArrowFlightClient getBasicClientAuthenticated(
      BufferAllocator allocator,
      String host, int port, String username,
      @Nullable String password, @Nullable HeaderCallOption clientProperties) {

    ClientIncomingAuthHeaderMiddleware.Factory factory =
        new ClientIncomingAuthHeaderMiddleware.Factory(
            new ClientBearerHeaderHandler());

    FlightClient flightClient = FlightClient.builder().allocator(allocator)
        .location(
            Location.forGrpcInsecure(host, port))
        .intercept(factory).build();

    return new ArrowFlightClient(flightClient, getAuthenticate(flightClient,
        username, password, factory, clientProperties));
  }

  /**
   * Creates a {@code ArrowFlightClient} wrapping a {@link FlightClient}
   * connected to the Dremio server without any encryption.
   *
   * @param allocator
   *          The buffer allocator to use for the {@code FlightClient} wrapped
   *          by this.
   * @param host
   *          The host to connect to.
   * @param port
   *          The port to connect to.
   * @param clientProperties
   *          The client properties to set during authentication.
   * @return a new {@code ArrowFlightClient} wrapping a non-encrypted
   *         {@code FlightClient}, with a bearer token for subsequent requests
   *         to the wrapped client.
   */
  public static ArrowFlightClient getBasicClientNoAuth(
      BufferAllocator allocator,
      String host, int port, @Nullable HeaderCallOption clientProperties) {

    FlightClient flightClient = FlightClient.builder().allocator(allocator)
        .location(
            Location.forGrpcInsecure(host, port)).build();

    return new ArrowFlightClient(flightClient, null);
  }

  /**
   * Creates a {@code ArrowFlightClient} wrapping a {@link FlightClient}
   * connected to the Dremio server with an encrypted TLS connection.
   *
   * @param allocator
   *          The buffer allocator to use for the {@code FlightClient} wrapped
   *          by this.
   * @param host
   *          The host to connect to.
   * @param port
   *          The port to connect to.
   * @param clientProperties
   *          The client properties to set during authentication.
   * @param username
   *          The username to connect with.
   * @param password
   *          The password to connect with.
   * @param keyStorePath
   *          The KeyStore path to use.
   * @param keyStorePass
   *          The KeyStore password to use.
   * @return a new {@code ArrowFlightClient} wrapping a non-encrypted
   *         {@code FlightClient}, with a bearer token for subsequent requests
   *         to the wrapped client.
   * @throws KeyStoreException
   *           If an error occurs while trying to retrieve KeyStore information.
   * @throws NoSuchAlgorithmException
   *           If a particular cryptographic algorithm is required but does not
   *           exist.
   * @throws CertificateException
   *           If an error occurs while trying to retrieve certificate
   *           information.
   * @throws IOException
   *           If an I/O operation fails.
   */
  public static ArrowFlightClient getEncryptedClientAuthenticated(
      BufferAllocator allocator,
      String host, int port,
      @Nullable HeaderCallOption clientProperties, String username,
      @Nullable String password, String keyStorePath, String keyStorePass)
      throws SQLException {

    try {

      ClientIncomingAuthHeaderMiddleware.Factory factory =
          new ClientIncomingAuthHeaderMiddleware.Factory(
              new ClientBearerHeaderHandler());

      FlightClient flightClient = FlightClient.builder().allocator(allocator)
          .location(
              Location.forGrpcTls(host, port))
          .intercept(factory).useTls()
          .trustedCertificates(getCertificateStream(keyStorePath, keyStorePass))
          .build();

      return new ArrowFlightClient(flightClient, getAuthenticate(flightClient,
          username, password, factory, clientProperties));
    } catch (Exception e) {
      throw new SQLException(
          "Failed to create a new Arrow Flight client.", e);
    }
  }

  /**
   * Creates a {@code ArrowFlightClient} wrapping a {@link FlightClient}
   * connected to the Dremio server with an encrypted TLS connection.
   *
   * @param allocator
   *          The buffer allocator to use for the {@code FlightClient} wrapped
   *          by this.
   * @param host
   *          The host to connect to.
   * @param port
   *          The port to connect to.
   * @param clientProperties
   *          The client properties to set during authentication.
   * @param keyStorePath
   *          The KeyStore path to use.
   * @param keyStorePass
   *          The KeyStore password to use.
   * @return a new {@code ArrowFlightClient} wrapping a non-encrypted
   *         {@code FlightClient}, with a bearer token for subsequent requests
   *         to the wrapped client.
   * @throws KeyStoreException
   *           If an error occurs while trying to retrieve KeyStore information.
   * @throws NoSuchAlgorithmException
   *           If a particular cryptographic algorithm is required but does not
   *           exist.
   * @throws CertificateException
   *           If an error occurs while trying to retrieve certificate
   *           information.
   * @throws IOException
   *           If an I/O operation fails.
   */
  public static ArrowFlightClient getEncryptedClientNoAuth(
      BufferAllocator allocator,
      String host, int port,
      @Nullable HeaderCallOption clientProperties, String keyStorePath,
      String keyStorePass)
      throws SQLException {

    try {

      FlightClient flightClient = FlightClient.builder().allocator(allocator)
          .location(
              Location.forGrpcTls(host, port)).useTls()
          .trustedCertificates(getCertificateStream(keyStorePath, keyStorePass))
          .build();

      return new ArrowFlightClient(flightClient, null);
    } catch (KeyStoreException | NoSuchAlgorithmException |
          CertificateException | IOException e) {
      throw new SQLException("Failed to create an Arrow Flight client.", e);
    }
  }

  /**
   * Helper method to authenticate provided {@link FlightClient} instance
   * against an Arrow Flight server endpoint.
   *
   * @param client
   *          the FlightClient instance to connect to Arrow Flight.
   * @param username
   *          the Arrow Flight server username.
   * @param password
   *          the corresponding Arrow Flight server password
   * @param factory
   *          the factory to create {@link ClientIncomingAuthHeaderMiddleware}.
   * @param clientProperties
   *          client properties to set during authentication.
   * @return {@link CredentialCallOption} encapsulating the bearer token to use
   *         in subsequent requests.
   */
  public static CredentialCallOption getAuthenticate(FlightClient client,
      String username, @Nullable String password,
      ClientIncomingAuthHeaderMiddleware.Factory factory,
      @Nullable HeaderCallOption clientProperties) {

    final List<CallOption> callOptions = new ArrayList<>();

    callOptions.add(new CredentialCallOption(
        new BasicAuthCredentialWriter(username, password)));

    if (clientProperties != null) {
      callOptions.add(clientProperties);
    }

    client.handshake(callOptions.toArray(new CallOption[callOptions.size()]));

    return factory.getCredentialCallOption();
  }

  /**
   * Generates an {@link InputStream} that contains certificates for a private
   * key.
   *
   * @param keyStorePath
   *          The path to the keystore.
   * @param keyStorePass
   *          The password for the keystore.
   * @return a new {code InputStream} containing the certificates.
   * @throws Exception
   *           If there was an error looking up the private key or certificates.
   */
  public static InputStream getCertificateStream(String keyStorePath,
      String keyStorePass) throws KeyStoreException, NoSuchAlgorithmException,
      CertificateException, IOException {

    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    try (final InputStream keyStoreStream = Files
        .newInputStream(Paths.get(Preconditions.checkNotNull(keyStorePath)))) {
      keyStore.load(keyStoreStream,
          Preconditions.checkNotNull(keyStorePass).toCharArray());
    }

    Enumeration<String> aliases = keyStore.aliases();

    while (aliases.hasMoreElements()) {
      final String alias = aliases.nextElement();
      if (keyStore.isCertificateEntry(alias)) {
        final Certificate certificates = keyStore.getCertificate(alias);
        return toInputStream(certificates);
      }
    }

    throw new RuntimeException("Keystore did not have a certificate.");
  }

  private static InputStream toInputStream(Certificate certificate)
      throws IOException {

    try (final StringWriter writer = new StringWriter();
        final JcaPEMWriter pemWriter = new JcaPEMWriter(writer)) {

      pemWriter.writeObject(certificate);
      pemWriter.flush();
      return new ByteArrayInputStream(
          writer.toString().getBytes(StandardCharsets.UTF_8));
    }
  }

  @Override
  public void close() throws Exception {
    try {
      client.close();
    } catch (InterruptedException e) {
      System.out.println("[WARNING] Failed to close resource.");
      e.printStackTrace();
    }
  }
}

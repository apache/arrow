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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
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
import org.apache.calcite.avatica.org.apache.http.auth.UsernamePasswordCredentials;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

/**
 * An adhoc {@link FlightClient} wrapper used to access the client. Allows for
 * the reuse of credentials.
 */
public final class ArrowFlightClient {

  private final FlightClient client;

  // TODO This will be used later in order to run queries.
  @SuppressWarnings("unused")
  private final CredentialCallOption properties;

  private ArrowFlightClient(FlightClient client,
      CredentialCallOption properties) {
    this.client = client;
    this.properties = properties;
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
   * @param address
   *          The {@link URI} corresponding to the location of the server.
   * @param credentials
   *          The username and password to authenticated to the client with.
   * @param clientProperties
   *          The client properties to set during authentication.
   * @return a new {@code ArrowFlightClient} wrapping a non-encrypted
   *         {@code FlightClient}, with a bearer token for subsequent requests
   *         to the wrapped client.
   */
  public static ArrowFlightClient getBasicClient(BufferAllocator allocator,
      URI address, UsernamePasswordCredentials credentials,
      @Nullable HeaderCallOption clientProperties) {

    ClientIncomingAuthHeaderMiddleware.Factory factory =
        new ClientIncomingAuthHeaderMiddleware.Factory(
            new ClientBearerHeaderHandler());
    FlightClient flightClient = FlightClient.builder().allocator(allocator)
        .location(
            Location.forGrpcInsecure(address.getHost(), address.getPort()))
        .intercept(factory).build();
    return new ArrowFlightClient(flightClient,
        getAuthenticate(flightClient, credentials.getUserName(),
            credentials.getPassword(), factory, clientProperties));
  }

  /**
   * Creates a {@code ArrowFlightClient} wrapping a {@link FlightClient}
   * connected to the Dremio server without any encryption.
   *
   * @param allocator
   *          The buffer allocator to use for the {@code FlightClient} wrapped
   *          by this.
   * @param address
   *          The {@link URI} corresponding to the location of the server.
   * @param credentials
   *          The username and password to authenticated to the client with.
   * @return a new {@code ArrowFlightClient} wrapping a non-encrypted
   *         {@code FlightClient}, with a bearer token for subsequent requests
   *         to the wrapped client.
   */
  public static ArrowFlightClient getBasicClient(BufferAllocator allocator,
      URI address, UsernamePasswordCredentials credentials)
      throws KeyStoreException, NoSuchAlgorithmException, CertificateException,
      IOException {
    return getBasicClient(allocator, address, credentials, null);
  }

  /**
   * Creates a {@code ArrowFlightClient} wrapping a {@link FlightClient}
   * connected to the Dremio server with an encrypted TLS connection.
   *
   * @param allocator
   *          The buffer allocator to use for the {@code FlightClient} wrapped
   *          by this.
   * @param address
   *          The {@link URI} corresponding to the location of the server.
   * @param clientProperties
   *          The client properties to set during authentication.
   * @param credentials
   *          The username and password to authenticated to the client with.
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
  public static ArrowFlightClient getEncryptedClient(BufferAllocator allocator,
      URI address, @Nullable HeaderCallOption clientProperties,
      UsernamePasswordCredentials credentials, String keyStorePath,
      String keyStorePass) throws KeyStoreException, NoSuchAlgorithmException,
      CertificateException, IOException {
    ClientIncomingAuthHeaderMiddleware.Factory factory =
        new ClientIncomingAuthHeaderMiddleware.Factory(
            new ClientBearerHeaderHandler());

    FlightClient flightClient = FlightClient.builder().allocator(allocator)
        .location(Location.forGrpcTls(address.getHost(), address.getPort()))
        .intercept(factory).useTls()
        .trustedCertificates(getCertificateStream(keyStorePath, keyStorePass))
        .build();
    return new ArrowFlightClient(flightClient,
        getAuthenticate(flightClient, credentials.getUserName(),
            credentials.getPassword(), factory, clientProperties));
  }

  /**
   * Creates a {@code ArrowFlightClient} wrapping a {@link FlightClient}
   * connected to the Dremio server with an encrypted TLS connection.
   *
   * @param allocator
   *          The buffer allocator to use for the {@code FlightClient} wrapped
   *          by this.
   * @param address
   *          The {@link URI} corresponding to the location of the server.
   * @param credentials
   *          The username and password to authenticated to the client with.
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
  public static ArrowFlightClient getEncryptedClient(BufferAllocator allocator,
      URI address, UsernamePasswordCredentials credentials, String keyStorePath,
      String keyStorePass) throws KeyStoreException, NoSuchAlgorithmException,
      CertificateException, IOException {
    return getEncryptedClient(allocator, address, null, credentials,
        keyStorePath, keyStorePass);
  }

  /**
   * Helper method to authenticate provided {@link FlightClient} instance
   * against an Arrow Flight server endpoint.
   *
   * @param client
   *          the FlightClient instance to connect to Arrow Flight.
   * @param user
   *          the Arrow Flight server username.
   * @param pass
   *          the corresponding Arrow Flight server password
   * @param factory
   *          the factory to create {@link ClientIncomingAuthHeaderMiddleware}.
   * @param clientProperties
   *          client properties to set during authentication.
   * @return {@link CredentialCallOption} encapsulating the bearer token to use
   *         in subsequent requests.
   */
  public static final CredentialCallOption getAuthenticate(FlightClient client,
      String user, String pass,
      ClientIncomingAuthHeaderMiddleware.Factory factory,
      HeaderCallOption clientProperties) {
    final List<CallOption> callOptions = new ArrayList<>();

    callOptions.add(
        new CredentialCallOption(new BasicAuthCredentialWriter(user, pass)));

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
  public static final InputStream getCertificateStream(String keyStorePath,
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

    throw new RuntimeException("Keystore did not have a private key.");
  }

  private static final InputStream toInputStream(Certificate certificate)
      throws IOException {

    try (final StringWriter writer = new StringWriter();
        final JcaPEMWriter pemWriter = new JcaPEMWriter(writer)) {

      pemWriter.writeObject(certificate);
      pemWriter.flush();
      return new ByteArrayInputStream(
          writer.toString().getBytes(StandardCharsets.UTF_8));
    }
  }
}

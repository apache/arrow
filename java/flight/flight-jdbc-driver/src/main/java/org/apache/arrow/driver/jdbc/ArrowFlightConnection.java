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
import java.net.URISyntaxException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;

/**
 * Connection to the Arrow Flight server.
 */
public final class ArrowFlightConnection extends AvaticaConnection {

  private static final String HOST = "host";
  private static final String PORT = "port";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  private static final String USE_TLS = "useTls";
  private static final String KEYSTORE_PATH = "keyStorePath";
  private static final String KEYSTORE_PASS = "keyStorePass";

  private BufferAllocator allocator;

  private ArrowFlightClient client;

  private final Map<Integer, ArrowFlightStatement> statementMap = new HashMap<>();

  /**
   * Instantiates a new Arrow Flight Connection.
   *
   * @param driver The JDBC driver to use.
   * @param factory The Avatica Factory to use.
   * @param url The URL to connect to.
   * @param info The properties of this connection.
   * @throws SQLException If the connection cannot be established.
   */
  public ArrowFlightConnection(ArrowFlightJdbcDriver driver,
      ArrowFlightFactory factory, String url, Properties info) throws SQLException {
    super(driver, factory, url, info);
    allocator = new RootAllocator(
        Integer.MAX_VALUE);
    
    try {
      loadClient();
    } catch (SQLException e) {
      allocator.close();
      throw e;
    }
  }

  /**
   * Gets the Flight Client.
   *
   * @return the {@link ArrowFlightClient} wrapped by this.
   */
  protected ArrowFlightClient getClient() {
    return client;
  }

  /**
   * Registers a statement to this connection, mapping it to its own
   * {@link ArrowFlightStatement#getId}.
   *
   * @param statement
   *          The {@code ArrowFlightStatement} to register to this connection.
   */
  public void addStatement(ArrowFlightStatement statement) {
    Preconditions.checkNotNull(
        statementMap.putIfAbsent(statement.getId(), statement),
        "Cannot register the same statement twice.");
  }

  /**
   * Returns the statement mapped to the provided ID.
   *
   * @param id
   *          The {@link ArrowFlightStatement#getId} from which to get the
   *          corresponding statement.
   * @return the {@link ArrowFlightStatement} mapped to the provided {@code id}
   */
  public ArrowFlightStatement getStatement(int id) {
    return Preconditions.checkNotNull(statementMap.get(id),
        "There is no such statement registed in this.");
  }

  /**
   * Sets {@link #client} based on the properties of this connection.
   *
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
   * @throws NumberFormatException
   *           If the port number to connect to is invalid.
   * @throws URISyntaxException
   *           If the URI syntax is invalid.
   */
  private void loadClient() throws SQLException {

    if (client != null) {
      throw new IllegalStateException("Client already loaded.");
    }

    String host = (String) info.getOrDefault(HOST, "localhost");
    Preconditions.checkArgument(!host.trim().isEmpty());

    int port = Integer.parseInt((String) info.getOrDefault(PORT, "32010"));
    Preconditions.checkArgument(port > 0);

    @Nullable
    String username = info.getProperty(USER);

    @Nullable
    String password = info.getProperty(PASSWORD);

    boolean useTls = ((String) info.getOrDefault(USE_TLS, "false"))
        .equalsIgnoreCase("true");
    
    boolean authenticate = username != null;

    if (!useTls) {

      if (authenticate) {
        client = ArrowFlightClient.getBasicClientAuthenticated(allocator, host,
            port, username, password, null);
        return;
      }

      client = ArrowFlightClient.getBasicClientNoAuth(allocator, host, port,
          null);
      return;

    }

    String keyStorePath = info.getProperty(KEYSTORE_PATH);
    String keyStorePass = info.getProperty(KEYSTORE_PASS);

    if (authenticate) {
      client = ArrowFlightClient.getEncryptedClientAuthenticated(allocator,
          host, port, null, username, password, keyStorePath, keyStorePass);
      return;
    }

    client = ArrowFlightClient.getEncryptedClientNoAuth(allocator, host,
        port, null, keyStorePath, keyStorePass);
  }

  @Override
  public void close() throws SQLException {
    try {
      client.close();
    } catch (Exception e) {
      throw new SQLException(
          "Failed to close the connection " +
              "to the Arrow Flight client.", e);
    }

    try {
      allocator.close();
    } catch (Exception e) {
      throw new SQLException("Failed to close the resource allocator used " +
          "by the Arrow Flight client.", e);
    }

    super.close();
  }

}

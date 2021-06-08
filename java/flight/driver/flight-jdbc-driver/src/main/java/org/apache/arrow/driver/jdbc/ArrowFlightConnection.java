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

  private final BufferAllocator allocator = new RootAllocator(
      Integer.MAX_VALUE);

  private ArrowFlightClient client;

  private final Map<Integer, ArrowFlightStatement> statementMap = new HashMap<>();

  public ArrowFlightConnection(ArrowFlightJdbcDriver driver,
      ArrowFlightFactory factory, String url, Properties info)
      throws SQLException {
    super(driver, factory, url, info);
    loadClient();
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

    String host, username;

    host = (String) info.getOrDefault("host", "localhost");
    Preconditions.checkArgument(!host.trim().equals(""));

    int port = (int) info.getOrDefault("port", "32010");
    Preconditions.checkArgument(port > 0);

    username = Preconditions.checkNotNull(info.getProperty("user"));
    Preconditions.checkArgument(!username.trim().equals(""));

    @Nullable
    String password = info.getProperty("password");

    boolean useTls = ((String) info.getOrDefault("useTls", "false"))
        .equalsIgnoreCase("true");

    if (useTls) {
      String keyStorePath, keyStorePass;

      keyStorePath = info.getProperty("keyStorePath");
      keyStorePass = info.getProperty("keyStorePass");

      client = ArrowFlightClient.getEncryptedClient(allocator, host, port, null,
          username, password, keyStorePath, keyStorePass);
    }

    client = ArrowFlightClient.getBasicClient(allocator, host, port, username,
        password, null);
  }

  @Override
  public void close() throws SQLException {
    try {
      client.close();
    } catch (Exception e) {
      throw new SQLException(
          "Failed to close the connection to the Arrow Flight client: "
              + e.getMessage());
    }

    try {
      allocator.close();
    } catch (Exception e) {
      throw new SQLException("Failed to close the resource allocator used "
          + "by the Arrow Flight client: " + e.getMessage());
    }

    super.close();
  }

}

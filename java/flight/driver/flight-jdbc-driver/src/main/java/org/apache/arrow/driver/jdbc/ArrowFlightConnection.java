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
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.avatica.org.apache.http.auth.UsernamePasswordCredentials;

/**
 * Connection to the Arrow Flight server.
 */
public final class ArrowFlightConnection extends AvaticaConnection {

  private static final BufferAllocator allocator = new RootAllocator(
      Integer.MAX_VALUE);

  // TODO Use this later to run queries.
  @SuppressWarnings("unused")
  private ArrowFlightClient client;

  private final Map<Integer, ArrowFlightStatement> statementMap = new HashMap<>();

  public ArrowFlightConnection(UnregisteredDriver driver,
      AvaticaFactory factory, String url, Properties info)
      throws KeyStoreException, NoSuchAlgorithmException, CertificateException,
      IOException, NumberFormatException, URISyntaxException {
    super(driver, factory, url, info);
    loadClient();
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
  private void loadClient()
      throws KeyStoreException, NoSuchAlgorithmException, CertificateException,
      IOException, NumberFormatException, URISyntaxException {

    URI address = new URI(/* FIXME scheme= */"jdbc",
        info.getProperty("user") + ":" + info.getProperty("pass"),
        info.getProperty("host"), Integer.parseInt(info.getProperty("port")),
        null, null, null);

    UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
        info.getProperty("user"), info.getProperty("pass"));

    if (info.getProperty("useTls") != null && info.getProperty("useTls").equalsIgnoreCase("true")) {
      client = ArrowFlightClient.getEncryptedClient(allocator, address,
          credentials, info.getProperty("keyStorePath"),
          info.getProperty("keyStorePass"));
      return;
    }

    client = ArrowFlightClient.getBasicClient(allocator, address, credentials);
  }

}

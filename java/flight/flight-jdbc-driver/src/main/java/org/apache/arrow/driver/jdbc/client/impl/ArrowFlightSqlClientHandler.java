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

package org.apache.arrow.driver.jdbc.client.impl;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.arrow.driver.jdbc.client.ArrowFlightClientHandler;
import org.apache.arrow.driver.jdbc.client.FlightClientHandler;
import org.apache.arrow.driver.jdbc.client.utils.ClientCreationUtils;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

/**
 * A {@link FlightClientHandler} for a {@link FlightSqlClient}.
 */
public final class ArrowFlightSqlClientHandler extends ArrowFlightClientHandler {

  private final FlightSqlClient sqlClient;

  ArrowFlightSqlClientHandler(final FlightClient client, final FlightSqlClient sqlClient,
                              final CallOption... options) {
    super(client, options);
    this.sqlClient = Preconditions.checkNotNull(sqlClient);
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param host      the host to use.
   * @param port      the port to use.
   * @param username  the username to use.
   * @param password  the password to use.
   * @param allocator the {@link BufferAllocator}.
   * @param useTls    whether to use TLS encryption.
   * @param options   the options.
   * @return a new {@link ArrowFlightSqlClientHandler} based upon the aforementioned information.
   * @throws GeneralSecurityException If a certificate-related error occurs.
   * @throws IOException              If an error occurs while trying to establish a connection to the
   *                                  client.
   */
  public static ArrowFlightSqlClientHandler createNewHandler(final String host, final int port,
                                                             final String username, final String password,
                                                             final String keyStorePath, final String keyStorePassword,
                                                             final BufferAllocator allocator, final boolean useTls,
                                                             final CallOption... options)
      throws GeneralSecurityException, IOException {
    return createNewHandler(
        host, port, username, password, keyStorePath, keyStorePassword, allocator, useTls, Arrays.asList(options));
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param host             the host to use.
   * @param port             the port to use.
   * @param username         the username to use.
   * @param password         the password to use.
   * @param keyStorePath     the KeyStore path to use.
   * @param keyStorePassword the keyStore password to use.
   * @param allocator        the {@link BufferAllocator}.
   * @param useTls           whether to use TLS encryption.
   * @param options          the options.
   * @return a new {@link ArrowFlightSqlClientHandler} based upon the aforementioned information.
   * @throws GeneralSecurityException If a certificate-related error occurs.
   * @throws IOException              If an error occurs while trying to establish a connection to the
   *                                  client.
   */
  public static ArrowFlightSqlClientHandler createNewHandler(final String host, final int port,
                                                             final String username, final String password,
                                                             final String keyStorePath, final String keyStorePassword,
                                                             final BufferAllocator allocator,
                                                             final boolean useTls,
                                                             final Collection<CallOption> options)
      throws GeneralSecurityException, IOException {
    return createNewHandler(
        ClientCreationUtils.createAndGetClientInfo(
            host, port, username, password, keyStorePath,
            keyStorePassword, allocator, useTls, options));
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param clientInfo the client info.
   * @return a new {@link ArrowFlightSqlClientHandler} based upon the aforementioned information.
   */
  public static ArrowFlightSqlClientHandler createNewHandler(final Entry<FlightClient, CallOption[]> clientInfo) {
    return createNewHandler(clientInfo.getKey(), clientInfo.getValue());
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param client  the client.
   * @param options the options.
   * @return a new {@link ArrowFlightSqlClientHandler} based upon the aforementioned information.
   */
  public static ArrowFlightSqlClientHandler createNewHandler(final FlightClient client, final CallOption... options) {
    return new ArrowFlightSqlClientHandler(client, new FlightSqlClient(client), options);
  }

  @Override
  public List<FlightStream> getStreams(String query) {
    return getInfo(query).getEndpoints().stream()
        .map(FlightEndpoint::getTicket)
        .map(ticket -> sqlClient.getStream(ticket, getOptions()))
        .collect(Collectors.toList());
  }

  @Override
  public FlightInfo getInfo(String query) {
    return sqlClient.execute(query, getOptions());
  }

  @Override
  public void close() throws SQLException {
    try {
      super.close();
    } catch (final Exception e) {
      throw new SQLException("Failed to clean up resources.", e);
    }
  }
}

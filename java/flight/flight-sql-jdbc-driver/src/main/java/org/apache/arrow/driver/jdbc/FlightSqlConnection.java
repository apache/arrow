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

import static org.apache.arrow.driver.jdbc.utils.FlightSqlConnectionConfigImpl.ArrowFlightConnectionProperty.replaceSemiColons;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.driver.jdbc.client.FlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.utils.FlightSqlConnectionConfigImpl;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Connection to the Flight SQL server.
 */
public final class FlightSqlConnection extends AvaticaConnection {

  private final BufferAllocator allocator;
  private final FlightSqlClientHandler clientHandler;
  private final FlightSqlConnectionConfigImpl config;
  private ExecutorService executorService;

  /**
   * Creates a new {@link FlightSqlConnection}.
   *
   * @param driver        the {@link FlightSqlJdbcDriver} to use.
   * @param factory       the {@link AvaticaFactory} to use.
   * @param url           the URL to use.
   * @param properties    the {@link Properties} to use.
   * @param config        the {@link FlightSqlConnectionConfigImpl} to use.
   * @param allocator     the {@link BufferAllocator} to use.
   * @param clientHandler the {@link FlightSqlClientHandler} to use.
   */
  private FlightSqlConnection(final FlightSqlJdbcDriver driver, final AvaticaFactory factory,
                              final String url, final Properties properties,
                              final FlightSqlConnectionConfigImpl config,
                              final BufferAllocator allocator,
                              final FlightSqlClientHandler clientHandler) {
    super(driver, factory, url, properties);
    this.config = Preconditions.checkNotNull(config, "Config cannot be null.");
    this.allocator = Preconditions.checkNotNull(allocator, "Allocator cannot be null.");
    this.clientHandler = Preconditions.checkNotNull(clientHandler, "Handler cannot be null.");
  }

  /**
   * Creates a new {@link FlightSqlConnection} to a {@link FlightClient}.
   *
   * @param driver     the {@link FlightSqlJdbcDriver} to use.
   * @param factory    the {@link AvaticaFactory} to use.
   * @param url        the URL to establish the connection to.
   * @param properties the {@link Properties} to use for this session.
   * @param allocator  the {@link BufferAllocator} to use.
   * @return a new {@link FlightSqlConnection}.
   * @throws SQLException on error.
   */
  static FlightSqlConnection createNewConnection(final FlightSqlJdbcDriver driver,
                                                 final AvaticaFactory factory,
                                                 String url, final Properties properties,
                                                 final BufferAllocator allocator)
      throws SQLException {
    url = replaceSemiColons(url);
    final FlightSqlConnectionConfigImpl config = new FlightSqlConnectionConfigImpl(properties);
    final FlightSqlClientHandler clientHandler = createNewClientHandler(config, allocator);
    return new FlightSqlConnection(driver, factory, url, properties, config, allocator, clientHandler);
  }

  private static FlightSqlClientHandler createNewClientHandler(
      final FlightSqlConnectionConfigImpl config,
      final BufferAllocator allocator) throws SQLException {
    try {
      return new FlightSqlClientHandler.Builder()
          .withHost(config.getHost())
          .withPort(config.getPort())
          .withUsername(config.getUser())
          .withPassword(config.getPassword())
          .withTrustStorePath(config.getTrustStorePath())
          .withTrustStorePassword(config.getTrustStorePassword())
          .withSystemTrustStore(config.useSystemTrustStore())
          .withBufferAllocator(allocator)
          .withEncryption(config.useEncryption())
          .withDisableCertificateVerification(config.getDisableCertificateVerification())
          .withToken(config.getToken())
          .withCallOptions(config.toCallOption())
          .build();
    } catch (final SQLException e) {
      try {
        allocator.close();
      } catch (final Exception allocatorCloseEx) {
        e.addSuppressed(allocatorCloseEx);
      }
      throw e;
    }
  }

  void reset() throws SQLException {
    // Clean up any open Statements
    try {
      AutoCloseables.close(statementMap.values());
    } catch (final Exception e) {
      throw AvaticaConnection.HELPER.createException(e.getMessage(), e);
    }

    statementMap.clear();

    // Reset Holdability
    this.setHoldability(this.metaData.getResultSetHoldability());

    // Reset Meta
    ((FlightSqlMetaImpl) this.meta).setDefaultConnectionProperties();
  }

  /**
   * Gets the client {@link #clientHandler} backing this connection.
   *
   * @return the handler.
   */
  FlightSqlClientHandler getClientHandler() throws SQLException {
    return clientHandler;
  }

  /**
   * Gets the {@link ExecutorService} of this connection.
   *
   * @return the {@link #executorService}.
   */
  synchronized ExecutorService getExecutorService() {
    return executorService = executorService == null ?
        Executors.newFixedThreadPool(config.threadPoolSize(),
            new DefaultThreadFactory(getClass().getSimpleName())) :
        executorService;
  }

  @Override
  public Properties getClientInfo() {
    final Properties copy = new Properties();
    copy.putAll(info);
    return copy;
  }

  @Override
  public void close() throws SQLException {
    if (executorService != null) {
      executorService.shutdown();
    }

    try {
      AutoCloseables.close(clientHandler);
      allocator.getChildAllocators().forEach(AutoCloseables::closeNoChecked);
      AutoCloseables.close(allocator);

      super.close();
    } catch (final Exception e) {
      throw AvaticaConnection.HELPER.createException(e.getMessage(), e);
    }
  }

  BufferAllocator getBufferAllocator() {
    return allocator;
  }

  public FlightSqlMetaImpl getMeta() {
    return (FlightSqlMetaImpl) this.meta;
  }
}

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

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.driver.jdbc.client.FlightClientHandler;
import org.apache.arrow.driver.jdbc.client.impl.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Connection to the Arrow Flight server.
 */
public final class ArrowFlightConnection extends AvaticaConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFlightConnection.class);
  private final BufferAllocator allocator;
  private final FlightClientHandler clientHandler;
  private final ArrowFlightConnectionConfigImpl config;
  private ExecutorService executorService;

  /**
   * Creates a new {@link ArrowFlightConnection}.
   *
   * @param driver        the {@link ArrowFlightJdbcDriver} to use.
   * @param factory       the {@link AvaticaFactory} to use.
   * @param url           the URL to use.
   * @param properties    the {@link Properties} to use.
   * @param config        the {@link ArrowFlightConnectionConfigImpl} to use.
   * @param allocator     the {@link BufferAllocator} to use.
   * @param clientHandler the {@link FlightClientHandler} to use.
   */
  private ArrowFlightConnection(final ArrowFlightJdbcDriver driver, final AvaticaFactory factory,
                                final String url, final Properties properties,
                                final ArrowFlightConnectionConfigImpl config,
                                final BufferAllocator allocator,
                                final FlightClientHandler clientHandler) {
    super(driver, factory, url, properties);
    this.config = Preconditions.checkNotNull(config, "Config cannot be null.");
    this.allocator = Preconditions.checkNotNull(allocator, "Allocator cannot be null.");
    this.clientHandler = Preconditions.checkNotNull(clientHandler, "Handler cannot be null.");
  }

  /**
   * Creates a new {@link ArrowFlightConnection} to a {@link FlightClient}.
   *
   * @param driver     the {@link ArrowFlightJdbcDriver} to use.
   * @param factory    the {@link AvaticaFactory} to use.
   * @param url        the URL to establish the connection to.
   * @param properties the {@link Properties} to use for this session.
   * @param allocator  the {@link BufferAllocator} to use.
   * @return a new {@link ArrowFlightConnection}.
   * @throws SQLException on error.
   */
  static ArrowFlightConnection createNewConnection(final ArrowFlightJdbcDriver driver,
                                                   final AvaticaFactory factory,
                                                   final String url, final Properties properties,
                                                   final BufferAllocator allocator)
      throws SQLException {
    try {
      final ArrowFlightConnectionConfigImpl config = new ArrowFlightConnectionConfigImpl(properties);
      final FlightClientHandler clientHandler =
          new ArrowFlightSqlClientHandler.Builder()
              .withHost(config.getHost())
              .withPort(config.getPort())
              .withUsername(config.avaticaUser())
              .withPassword(config.avaticaPassword())
              .withKeyStorePath(config.getKeyStorePath())
              .withKeyStorePassword(config.keystorePassword())
              .withBufferAllocator(allocator)
              .withTlsEncryption(config.useTls())
              .withCallOptions(config.toCallOption())
              .build();
      return new ArrowFlightConnection(driver, factory, url, properties, config, allocator, clientHandler);
    } catch (final SQLException e) {
      allocator.close();
      throw e;
    }
  }

  void reset() throws SQLException {
    final Set<SQLException> exceptions = new HashSet<>();
    // Clean up any open Statements
    for (final AvaticaStatement statement : statementMap.values()) {
      try {
        AutoCloseables.close(statement);
      } catch (final Exception e) {
        exceptions.add(AvaticaConnection.HELPER.createException(e.getMessage(), e));
      }
    }
    statementMap.clear();
    try {
      // Reset Holdability
      this.setHoldability(this.metaData.getResultSetHoldability());
    } catch (final SQLException e) {
      exceptions.add(e);
    }
    // Reset Meta
    ((ArrowFlightMetaImpl) this.meta).setDefaultConnectionProperties();
    if (!exceptions.isEmpty()) {
      final SQLException exception = AvaticaConnection.HELPER.createException("Failed to reset connection.");
      exceptions.forEach(exception::setNextException);
      throw exception;
    }
  }

  /**
   * Gets the client {@link #clientHandler} backing this connection.
   *
   * @return the handler.
   */
  FlightClientHandler getClientHandler() {
    return clientHandler;
  }

  /**
   * Gets the {@link ExecutorService} of this connection.
   *
   * @return the {@link #executorService}.
   */
  synchronized ExecutorService getExecutorService() {
    return executorService = executorService == null ?
        Executors.newFixedThreadPool(config.threadPoolSize(), new DefaultThreadFactory(getClass().getSimpleName())) :
        executorService;
  }

  @Override
  public Properties getClientInfo() {
    return info;
  }

  @Override
  public void close() throws SQLException {
    if (executorService != null) {
      executorService.shutdown();
    }

    final Set<SQLException> exceptions = new HashSet<>();

    try {
      AutoCloseables.close(clientHandler);
    } catch (final Exception e) {
      exceptions.add(AvaticaConnection.HELPER.createException(e.getMessage(), e));
    }

    try {
      allocator.getChildAllocators().forEach(AutoCloseables::closeNoChecked);
      AutoCloseables.close(allocator);
    } catch (final Exception e) {
      exceptions.add(AvaticaConnection.HELPER.createException(e.getMessage(), e));
    }

    try {
      super.close();
    } catch (final Exception e) {
      exceptions.add(AvaticaConnection.HELPER.createException(e.getMessage(), e));
    }
    if (exceptions.isEmpty()) {
      return;
    }
    final SQLException exception =
        AvaticaConnection.HELPER.createException("Failed to clean up resources; a memory leak will likely take place.");
    exceptions.forEach(exception::setNextException);
    /*
     * FIXME Properly close allocator held open with outstanding child allocators.
     * A bug has been detected in this code. Closing the connection does not release the resources
     * from the allocator appropriately. This should be fixed in a future patch.
     */
    LOGGER.error("Memory leak detected!", exception);
  }
}

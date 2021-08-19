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

import static java.lang.String.format;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.HOST;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.KEYSTORE_PASS;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.KEYSTORE_PATH;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.PASSWORD;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.PORT;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.USERNAME;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.USE_TLS;
import static org.apache.arrow.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.client.FlightClientHandler;
import org.apache.arrow.driver.jdbc.utils.BaseProperty;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.HeaderCallOption;
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
public class ArrowFlightConnection extends AvaticaConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFlightConnection.class);
  private final BufferAllocator allocator;
  private final PropertyManager manager;
  private final FlightClientHandler handler;
  private ExecutorService executorService;

  /**
   * Creates a new {@link ArrowFlightConnection}.
   *
   * @param driver    the {@link ArrowFlightJdbcDriver} to use.
   * @param factory   the {@link AvaticaFactory} to use.
   * @param url       the URL to establish the connection.
   * @param manager   the {@link PropertyManager} for this connection.
   * @param allocator the {@link BufferAllocator} to use.
   * @param handler   the {@link FlightClientHandler} to use.
   */
  protected ArrowFlightConnection(final ArrowFlightJdbcDriver driver, final AvaticaFactory factory,
                                  final String url, final PropertyManager manager,
                                  final BufferAllocator allocator, final FlightClientHandler handler) {
    super(
        driver,
        factory,
        url,
        Preconditions.checkNotNull(manager, "Manager cannot be null!").getProperties());
    this.allocator = Preconditions.checkNotNull(allocator, "Allocator cannot be null!");
    this.handler = Preconditions.checkNotNull(handler, "Handler cannot be null!");
    this.manager = manager;
  }

  /**
   * Creates a new {@link ArrowFlightConnection} to a {@link FlightClient}.
   *
   * @param driver    the {@link ArrowFlightJdbcDriver} to use.
   * @param factory   the {@link AvaticaFactory} to use.
   * @param url       the URL to establish the connection to.
   * @param info      the {@link Properties} to use for this session.
   * @param allocator the {@link BufferAllocator} to use.
   * @return a new {@link ArrowFlightConnection}.
   * @throws SQLException on error.
   */
  public static ArrowFlightConnection createNewConnection(final ArrowFlightJdbcDriver driver,
                                                          final AvaticaFactory factory,
                                                          final String url, final Properties info,
                                                          final BufferAllocator allocator)
      throws SQLException {
    final PropertyManager manager = new PropertyManager(info);
    final Entry<String, Integer> address =
        new SimpleImmutableEntry<>(manager.getPropertyAsString(HOST), manager.getPropertyAsInteger(PORT));
    final String username = manager.getPropertyAsString(USERNAME);
    final Entry<String, String> credentials =
        username == null ? null : new SimpleImmutableEntry<>(username, manager.getPropertyAsString(PASSWORD));
    final String keyStorePath = manager.getPropertyAsString(KEYSTORE_PATH);
    final Entry<String, String> keyStoreInfo =
        keyStorePath == null ? null :
            new SimpleImmutableEntry<>(keyStorePath, manager.getPropertyAsString(KEYSTORE_PASS));
    try {
      final FlightClientHandler handler = ArrowFlightSqlClientHandler.createNewHandler(
          address, credentials, keyStoreInfo, allocator, manager.getPropertyAsBoolean(USE_TLS), manager.toCallOption());
      return new ArrowFlightConnection(driver, factory, url, manager, allocator, handler);
    } catch (final GeneralSecurityException | IOException e) {
      manager.close();
      throw AvaticaConnection.HELPER.createException("Failed to establish a valid connection to the Flight Client.", e);
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
    if (exceptions.isEmpty()) {
      return;
    }
    final SQLException exception = AvaticaConnection.HELPER.createException("Failed to reset connection.");
    exceptions.forEach(exception::setNextException);
    throw exception;
  }

  /**
   * Gets the client {@link #handler} backing this connection.
   *
   * @return the handler.
   */
  protected final FlightClientHandler getHandler() {
    return handler;
  }

  /**
   * Gets the {@link ExecutorService} of this connection.
   *
   * @return the {@link #executorService}.
   */
  public synchronized ExecutorService getExecutorService() {
    if (executorService == null) {
      final int threadPoolSize = manager.getPropertyAsInteger(BaseProperty.THREAD_POOL_SIZE);
      final DefaultThreadFactory threadFactory = new DefaultThreadFactory(this.getClass().getSimpleName());
      executorService = Executors.newFixedThreadPool(threadPoolSize, threadFactory);
    }

    return executorService;
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
      AutoCloseables.close(handler, manager);
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

  /**
   * A property manager for the {@link ArrowFlightConnection}.
   */
  protected static final class PropertyManager implements AutoCloseable {
    private final Properties properties;

    public PropertyManager(final Properties properties) {
      this.properties = Preconditions.checkNotNull(properties);
    }

    /**
     * Gets the {@link #properties} managed by this wrapper.
     *
     * @return the properties.
     */
    public Properties getProperties() {
      return properties;
    }

    /**
     * Gets the {@link #properties} managed by this wrapper as a {@link CallOption}.
     *
     * @return the properties as a call option.
     */
    public CallOption toCallOption() {
      final CallHeaders headers = new FlightCallHeaders();
      properties.forEach((key, val) -> headers.insert(key.toString(), val.toString()));
      return new HeaderCallOption(headers);
    }

    /**
     * Gets the value mapped to the provided {@code property} key as a boolean value.
     *
     * @param property the property.
     * @return the property value as a boolean value.
     */
    public boolean getPropertyAsBoolean(final BaseProperty property) {
      final Object object = getPropertyOrDefault(checkNotNull(property));
      return object != null && Boolean.parseBoolean(object.toString());
    }

    /**
     * Gets the value mapped to the provided {@code property} key as a string value.
     *
     * @param property the property.
     * @return the property value as a string value.
     */
    public String getPropertyAsString(final BaseProperty property) {
      final Object object = getPropertyOrDefault(checkNotNull(property));
      return object == null ? null : object.toString();
    }

    /**
     * Gets the value mapped to the provided {@code property} key as a boolean value.
     *
     * @param property the property.
     * @return the property value as an integer value.
     */
    public int getPropertyAsInteger(final BaseProperty property) {
      final Object object = getPropertyOrDefault(checkNotNull(property));
      return object == null ? 0 : Integer.parseInt(object.toString());
    }

    /**
     * Gets the value mapped to the provided {@code property} key.
     *
     * @param property the property.
     * @return the property value.
     */
    public Object getPropertyOrDefault(final BaseProperty property) {
      return getPropertyOrDefault(property, Object.class);
    }

    /**
     * Gets the value mapped to the provided {@code property} key.
     *
     * @param property the property.
     * @return the property value.
     */
    public <T> T getPropertyOrDefault(final BaseProperty property, final Class<T> clazz) {
      final Object object = getProperties().getOrDefault(property.getName(), property.getDefaultValue());
      if (object == null) {
        return null;
      }
      final Class<?> objClass = object.getClass();
      Preconditions.checkState(
          clazz.isAssignableFrom(objClass),
          format("%s cannot be cast to %s!", objClass, clazz.getName()));
      return clazz.cast(object);
    }

    @Override
    public void close() {
      properties.clear();
    }
  }

}

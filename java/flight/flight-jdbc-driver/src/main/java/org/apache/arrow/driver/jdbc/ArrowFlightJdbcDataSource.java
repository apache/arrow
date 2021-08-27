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

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.PropertiesUtils;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.BuiltInConnectionProperty;

import com.google.common.collect.ImmutableMap;

/**
 * {@link DataSource} implementation for Arrow Flight JDBC Driver.
 */
public class ArrowFlightJdbcDataSource implements DataSource {
  private final Properties properties;
  private final ArrowFlightConnectionConfigImpl config;
  private PrintWriter logWriter;

  /**
   * Instantiates a new DataSource.
   */
  protected ArrowFlightJdbcDataSource(final Properties properties, final ArrowFlightConnectionConfigImpl config) {
    this.properties = Preconditions.checkNotNull(properties);
    this.config = Preconditions.checkNotNull(config);
  }

  /**
   * Gets the {@link #config} for this {@link ArrowFlightJdbcDataSource}.
   *
   * @return the {@link ArrowFlightConnectionConfigImpl}.
   */
  protected final ArrowFlightConnectionConfigImpl getConfig() {
    return config;
  }

  /**
   * Gets a copy of the {@link #properties} for this {@link ArrowFlightJdbcDataSource} with
   * the provided {@code username} and {@code password}.
   *
   * @return the {@link Properties} for this data source.
   */
  protected final Properties getProperties(final String username, final String password) {
    return PropertiesUtils.copyReplace(
        properties,
        ImmutableMap.of(
            BuiltInConnectionProperty.AVATICA_USER, username,
            BuiltInConnectionProperty.AVATICA_PASSWORD, password));
  }

  /**
   * Creates a new {@link ArrowFlightJdbcDataSource}.
   *
   * @param properties the properties.
   * @return a new data source.
   */
  public static ArrowFlightJdbcDataSource createNewDataSource(final Properties properties) {
    return new ArrowFlightJdbcDataSource(properties, new ArrowFlightConnectionConfigImpl(properties));
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return getConnection(config.avaticaUser(), config.avaticaPassword());
  }

  @Override
  public ArrowFlightConnection getConnection(final String username, final String password) throws SQLException {
    final Properties properties = new Properties();
    final BuiltInConnectionProperty user = BuiltInConnectionProperty.AVATICA_USER;
    final BuiltInConnectionProperty pass = BuiltInConnectionProperty.AVATICA_PASSWORD;
    properties.putAll(this.properties);
    properties.replace(user.camelName(), username == null ? user.defaultValue() : username);
    properties.replace(pass.camelName(), password == null ? pass.defaultValue() : password);
    return new ArrowFlightJdbcDriver().connect(config.url(), properties);
  }

  @Override
  public <T> T unwrap(Class<T> aClass) throws SQLException {
    throw new SQLException("ArrowFlightJdbcDataSource is not a wrapper.");
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    return false;
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return this.logWriter;
  }

  @Override
  public void setLogWriter(PrintWriter logWriter) throws SQLException {
    this.logWriter = logWriter;
  }

  @Override
  public void setLoginTimeout(int timeout) throws SQLException {
    throw new SQLFeatureNotSupportedException("Setting Login timeout is not supported.");
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return 0;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return Logger.getLogger("ArrowFlightJdbc");
  }
}

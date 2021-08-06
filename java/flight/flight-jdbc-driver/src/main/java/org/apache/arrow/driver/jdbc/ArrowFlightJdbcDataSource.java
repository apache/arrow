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
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.arrow.driver.jdbc.utils.BaseProperty;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.org.apache.http.client.utils.URIBuilder;

/**
 * {@link DataSource} implementation for Arrow Flight JDBC Driver.
 */
public class ArrowFlightJdbcDataSource implements DataSource {
  private final Properties properties;
  private PrintWriter logWriter;

  /**
   * Instantiates a new DataSource.
   */
  public ArrowFlightJdbcDataSource() {
    this.properties = new Properties();
    for (BaseProperty baseProperty : BaseProperty.values()) {
      Object defaultValue = baseProperty.getDefaultValue();
      if (defaultValue != null) {
        this.properties.put(baseProperty.getName(), defaultValue);
      }
    }
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return getConnection(getUsername(), getPassword());
  }

  @Override
  public ArrowFlightConnection getConnection(String username, String password) throws SQLException {
    final Properties properties = getProperties(username, password);
    return getConnection(properties);
  }

  ArrowFlightConnection getConnection(Properties properties) throws SQLException {
    final ArrowFlightJdbcDriver driver = new ArrowFlightJdbcDriver();

    final String connectionUrl = Preconditions.checkNotNull(getUrl());
    return (ArrowFlightConnection) driver.connect(connectionUrl, properties);
  }

  Properties getProperties(String username, String password) {
    final Properties properties = new Properties();
    properties.putAll(this.properties);
    properties.put(BaseProperty.USERNAME.getName(), username);
    properties.put(BaseProperty.PASSWORD.getName(), password);

    return properties;
  }

  private String getUrl() {
    try {
      return new URIBuilder()
          .setScheme("jdbc:arrow-flight")
          .setHost(getHost())
          .setPort(getPort())
          .build()
          .toString();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private Object getPropertyOrDefault(BaseProperty host) {
    return this.properties.getOrDefault(host.getName(), host.getDefaultValue());
  }

  /**
   * Sets the host used in this DataSource connections.
   * This will also update the connection URL.
   */
  public void setHost(String host) {
    this.properties.put(BaseProperty.HOST.getName(), host);
  }

  /**
   * Returns the host used in this DataSource connections.
   */
  public String getHost() {
    return (String) getPropertyOrDefault(BaseProperty.HOST);
  }

  /**
   * Sets the port used in this DataSource connections.
   * This will also update the connection URL.
   */
  public void setPort(int port) {
    this.properties.put(BaseProperty.PORT.getName(), port);
  }

  /**
   * Returns the port used in this DataSource connections.
   */
  public int getPort() {
    return (int) getPropertyOrDefault(BaseProperty.PORT);
  }

  /**
   * Sets the username used to authenticate the connections.
   */
  public void setUsername(String username) {
    this.properties.put(BaseProperty.USERNAME.getName(), username);
  }

  /**
   * Returns the username used to authenticate the connections.
   */
  public String getUsername() {
    return (String) getPropertyOrDefault(BaseProperty.USERNAME);
  }

  /**
   * Sets the password used to authenticate the connections.
   */
  public void setPassword(String password) {
    this.properties.put(BaseProperty.PASSWORD.getName(), password);
  }

  /**
   * Returns the password used to authenticate the connections.
   */
  public String getPassword() {
    return (String) getPropertyOrDefault(BaseProperty.PASSWORD);
  }

  /**
   * Enable or disable usage of TLS on FlightClient.
   */
  public void setUseTls(boolean useTls) {
    this.properties.put(BaseProperty.USE_TLS.getName(), useTls);
  }

  /**
   * Returns if usage of TLS is enabled on FlightClient.
   */
  public boolean getUseTls() {
    return (boolean) getPropertyOrDefault(BaseProperty.USE_TLS);
  }

  /**
   * Sets the key store path containing the trusted TLS certificates for the FlightClient.
   */
  public void setKeyStorePath(String keyStorePath) {
    this.properties.put(BaseProperty.KEYSTORE_PATH.getName(), keyStorePath);
    this.setUseTls(true);
  }

  /**
   * Returns the key store path containing the trusted TLS certificates for the FlightClient.
   */
  public String getKeyStorePath() {
    return (String) getPropertyOrDefault(BaseProperty.KEYSTORE_PATH);
  }

  /**
   * Sets the key store password containing the trusted TLS certificates for the FlightClient.
   */
  public void setKeyStorePass(String keyStorePass) {
    this.properties.put(BaseProperty.KEYSTORE_PASS.getName(), keyStorePass);
  }

  /**
   * Returns the key store password containing the trusted TLS certificates for the FlightClient.
   */
  public String getKeyStorePass() {
    return (String) getPropertyOrDefault(BaseProperty.KEYSTORE_PASS);
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

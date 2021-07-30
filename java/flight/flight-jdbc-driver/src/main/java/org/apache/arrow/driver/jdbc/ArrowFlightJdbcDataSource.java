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
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.arrow.driver.jdbc.utils.BaseProperty;
import org.apache.calcite.avatica.org.apache.http.client.utils.URIBuilder;

public class ArrowFlightJdbcDataSource implements DataSource {
  private String url;
  private final Properties properties;
  private PrintWriter logWriter;

  public ArrowFlightJdbcDataSource() {
    this.properties = new Properties();
  }

  @Override
  public Connection getConnection() throws SQLException {
    return getConnection(getUsername(), getPassword());
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    final ArrowFlightJdbcDriver driver = new ArrowFlightJdbcDriver();

    final Properties properties = new Properties(this.properties);
    properties.put(BaseProperty.USERNAME.getName(), username);
    properties.put(BaseProperty.PASSWORD.getName(), password);

    final String connectionUrl = getUrl();
    if (url == null) {
      throw new SQLException("Connection URL not set on ArrowFlightJdbcDataSource");
    }

    return driver.connect(connectionUrl, properties);
  }

  private void updateUrl() {
    try {
      this.url = new URIBuilder()
          .setScheme("jdbc:arrow-flight")
          .setHost(getHost())
          .setPort(getPort())
          .build()
          .toString();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public void setUrl(String url) {
    this.url = url;
    try {
      final URI uri = new URI(this.url);
      this.setProperty(BaseProperty.HOST, uri.getHost());
      this.setProperty(BaseProperty.PORT, uri.getPort());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public String getUrl() {
    return this.url;
  }

  private void setProperty(BaseProperty property, Object value) {
    this.properties.put(property.getName(), value);
  }

  private Object getPropertyOrDefault(BaseProperty host) {
    return this.properties.getOrDefault(host.getName(), host.getDefaultValue());
  }

  public void setHost(String host) {
    this.setProperty(BaseProperty.HOST, host);
    this.updateUrl();
  }


  public String getHost() {
    return (String) getPropertyOrDefault(BaseProperty.HOST);
  }

  public void setPort(int port) {
    this.setProperty(BaseProperty.PORT, port);
    this.updateUrl();
  }

  public int getPort() {
    return (int) getPropertyOrDefault(BaseProperty.PORT);
  }

  public void setUsername(String username) {
    this.setProperty(BaseProperty.USERNAME, username);
  }

  public String getUsername() {
    return (String) getPropertyOrDefault(BaseProperty.USERNAME);
  }

  public void setPassword(String password) {
    this.setProperty(BaseProperty.PASSWORD, password);
  }

  public String getPassword() {
    return (String) getPropertyOrDefault(BaseProperty.PASSWORD);
  }

  public void setEncrypt(boolean encrypt) {
    this.setProperty(BaseProperty.PASSWORD, String.valueOf(encrypt));
  }

  public boolean getEncrypt() {
    return Boolean.parseBoolean((String) getPropertyOrDefault(BaseProperty.ENCRYPT));
  }

  public void setKeyStorePath(String keyStorePath) {
    this.setProperty(BaseProperty.KEYSTORE_PATH, keyStorePath);
  }

  public String getKeyStorePath() {
    return (String) getPropertyOrDefault(BaseProperty.KEYSTORE_PATH);
  }

  public void setKeyStorePass(String keyStorePass) {
    this.setProperty(BaseProperty.KEYSTORE_PASS, keyStorePass);
  }

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

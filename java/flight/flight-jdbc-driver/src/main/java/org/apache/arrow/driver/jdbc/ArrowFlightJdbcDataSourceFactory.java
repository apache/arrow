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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.utils.BaseProperty;
import org.apache.calcite.avatica.org.apache.http.client.utils.URIBuilder;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;

/**
 * DataSource implementation for ArrowFlightJdbcDriver.
 */
public class ArrowFlightJdbcDataSourceFactory {

  public static final String DRIVER_CLASS_NAME = ArrowFlightJdbcDriver.class.getName();
  private final Properties properties = new Properties();

  /**
   * Instantiate a new DataSource.
   */
  public BasicDataSource createDataSource() throws Exception {
    loadDriver();

    final BasicDataSource dataSource = BasicDataSourceFactory.createDataSource(this.properties);
    dataSource.setDriverClassName(DRIVER_CLASS_NAME);
    dataSource.setUrl(buildUrl());
    dataSource.setUsername(getUsername());
    dataSource.setPassword(getPassword());

    return dataSource;
  }

  private String buildUrl() throws URISyntaxException {
    URI uri = new URIBuilder()
        .setScheme("jdbc:arrow-flight")
        .setHost(getHost())
        .setPort(getPort())
        .build();

    return uri.toString();
  }

  private static void loadDriver() {
    try {
      Class.forName(DRIVER_CLASS_NAME);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public void addConnectionProperty(String property, String value) {
    this.properties.put(property, value);
  }

  private String getPropertyOrDefault(BaseProperty host) {
    return (String) this.properties.getOrDefault(host.getName(), host.getDefaultValue());
  }

  private void setProperty(BaseProperty property, Object value) {
    this.properties.put(property.getName(), value);
  }

  public void setHost(String host) {
    this.setProperty(BaseProperty.HOST, host);
  }

  public String getHost() {
    return getPropertyOrDefault(BaseProperty.HOST);
  }

  public void setPort(int port) {
    this.setProperty(BaseProperty.PORT, String.valueOf(port));
  }

  public int getPort() {
    return Integer.parseInt(getPropertyOrDefault(BaseProperty.PORT));
  }

  public void setUsername(String username) {
    this.setProperty(BaseProperty.USERNAME, username);
  }

  public String getUsername() {
    return getPropertyOrDefault(BaseProperty.USERNAME);
  }

  public void setPassword(String password) {
    this.setProperty(BaseProperty.PASSWORD, password);
  }

  public String getPassword() {
    return getPropertyOrDefault(BaseProperty.PASSWORD);
  }

  public void setEncrypt(boolean encrypt) {
    this.setProperty(BaseProperty.PASSWORD, String.valueOf(encrypt));
  }

  public boolean getEncrypt() {
    return Boolean.parseBoolean(getPropertyOrDefault(BaseProperty.ENCRYPT));
  }

  public void setKeyStorePath(String keyStorePath) {
    this.setProperty(BaseProperty.KEYSTORE_PATH, keyStorePath);
  }

  public String getKeyStorePath() {
    return getPropertyOrDefault(BaseProperty.KEYSTORE_PATH);
  }

  public void setKeyStorePass(String keyStorePass) {
    this.setProperty(BaseProperty.KEYSTORE_PASS, keyStorePass);
  }

  public String getKeyStorePass() {
    return getPropertyOrDefault(BaseProperty.KEYSTORE_PASS);
  }
}

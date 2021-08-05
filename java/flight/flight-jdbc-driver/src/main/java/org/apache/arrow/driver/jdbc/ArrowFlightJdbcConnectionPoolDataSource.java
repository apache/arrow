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
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

/**
 * {@link ConnectionPoolDataSource} implementation for Arrow Flight JDBC Driver.
 */
public class ArrowFlightJdbcConnectionPoolDataSource
    implements ConnectionPoolDataSource, ConnectionEventListener, AutoCloseable {
  private final ArrowFlightJdbcDataSource dataSource = new ArrowFlightJdbcDataSource();
  private final Map<Credentials, Queue<PooledConnection>> pool = new ConcurrentHashMap<>();

  @Override
  public PooledConnection getPooledConnection() throws SQLException {
    return this.getPooledConnection(getUsername(), getPassword());
  }

  @Override
  public PooledConnection getPooledConnection(String username, String password) throws SQLException {
    Credentials credentials = new Credentials(username, password);
    Queue<PooledConnection> objectPool = pool.computeIfAbsent(credentials, s -> new ConcurrentLinkedQueue<>());
    PooledConnection pooledConnection = objectPool.poll();
    if (pooledConnection == null) {
      pooledConnection = createPooledConnection(username, password);
    }
    return pooledConnection;
  }

  private PooledConnection createPooledConnection(String username, String password) throws SQLException {
    Credentials credentials = new Credentials(username, password);
    ArrowFlightJdbcPooledConnection pooledConnection =
        new ArrowFlightJdbcPooledConnection(this.dataSource.getConnection(username, password), credentials);
    pooledConnection.addConnectionEventListener(this);
    return pooledConnection;
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return this.dataSource.getLogWriter();
  }

  @Override
  public void setLogWriter(PrintWriter logWriter) throws SQLException {
    this.dataSource.setLogWriter(logWriter);
  }

  @Override
  public void setLoginTimeout(int timeout) throws SQLException {
    this.dataSource.setLoginTimeout(timeout);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return this.dataSource.getLoginTimeout();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return this.dataSource.getParentLogger();
  }

  /**
   * Sets the host used in this DataSource connections.
   * This will also update the connection URL.
   */
  public void setHost(String host) {
    this.dataSource.setHost(host);
  }

  /**
   * Returns the host used in this DataSource connections.
   */
  public String getHost() {
    return this.dataSource.getHost();
  }

  /**
   * Sets the port used in this DataSource connections.
   * This will also update the connection URL.
   */
  public void setPort(int port) {
    this.dataSource.setPort(port);
  }

  /**
   * Returns the port used in this DataSource connections.
   */
  public int getPort() {
    return this.dataSource.getPort();
  }

  /**
   * Sets the username used to authenticate the connections.
   */
  public void setUsername(String username) {
    this.dataSource.setUsername(username);
  }

  /**
   * Returns the username used to authenticate the connections.
   */
  public String getUsername() {
    return this.dataSource.getUsername();
  }

  /**
   * Sets the password used to authenticate the connections.
   */
  public void setPassword(String password) {
    this.dataSource.setPassword(password);
  }

  /**
   * Returns the password used to authenticate the connections.
   */
  public String getPassword() {
    return this.dataSource.getPassword();
  }

  /**
   * Enable or disable usage of TLS on FlightClient.
   */
  public void setUseTls(boolean useTls) {
    this.dataSource.setUseTls(useTls);
  }

  /**
   * Returns if usage of TLS is enabled on FlightClient.
   */
  public boolean getUseTls() {
    return this.dataSource.getUseTls();
  }

  /**
   * Sets the key store path containing the trusted TLS certificates for the FlightClient.
   */
  public void setKeyStorePath(String keyStorePath) {
    this.dataSource.setKeyStorePass(keyStorePath);
  }

  /**
   * Returns the key store path containing the trusted TLS certificates for the FlightClient.
   */
  public String getKeyStorePath() {
    return this.dataSource.getKeyStorePath();
  }

  /**
   * Sets the key store password containing the trusted TLS certificates for the FlightClient.
   */
  public void setKeyStorePass(String keyStorePass) {
    this.dataSource.setKeyStorePass(keyStorePass);
  }

  /**
   * Returns the key store password containing the trusted TLS certificates for the FlightClient.
   */
  public String getKeyStorePass() {
    return this.dataSource.getKeyStorePass();
  }

  @Override
  public void connectionClosed(ConnectionEvent connectionEvent) {
    ArrowFlightJdbcPooledConnection pooledConnection = (ArrowFlightJdbcPooledConnection) connectionEvent.getSource();
    Credentials credentials = pooledConnection.getCredentials();

    this.pool.get(credentials).add(pooledConnection);
  }

  @Override
  public void connectionErrorOccurred(ConnectionEvent connectionEvent) {

  }

  @Override
  public void close() throws Exception {
    SQLException lastException = null;
    for (Queue<PooledConnection> connections : this.pool.values()) {
      while (!connections.isEmpty()) {
        PooledConnection pooledConnection = connections.poll();
        try {
          pooledConnection.close();
        } catch (SQLException e) {
          lastException = e;
        }
      }
    }

    if (lastException != null) {
      throw lastException;
    }
  }

  /**
   * Value object used as key of ArrowFlightJdbcConnectionPoolDataSource's connection pool.
   */
  static class Credentials {
    private final String username;
    private final String password;

    Credentials(String username, String password) {
      this.username = username;
      this.password = password;
    }

    @Override
    public int hashCode() {
      return username.hashCode() ^ password.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Credentials &&
          Objects.equals(this.username, ((Credentials) o).username) &&
          Objects.equals(this.password, ((Credentials) o).password);
    }
  }
}

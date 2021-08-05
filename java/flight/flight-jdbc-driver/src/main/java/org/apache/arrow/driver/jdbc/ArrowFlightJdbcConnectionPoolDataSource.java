package org.apache.arrow.driver.jdbc;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

public class ArrowFlightJdbcConnectionPoolDataSource implements ConnectionPoolDataSource, ConnectionEventListener {
  ArrowFlightJdbcDataSource dataSource = new ArrowFlightJdbcDataSource();

  @Override
  public PooledConnection getPooledConnection() throws SQLException {
    ArrowFlightJdbcPooledConnection pooledConnection = new ArrowFlightJdbcPooledConnection(this.dataSource.getConnection());
    pooledConnection.addConnectionEventListener(this);
    return pooledConnection;
  }

  @Override
  public PooledConnection getPooledConnection(String username, String password) throws SQLException {
    return new ArrowFlightJdbcPooledConnection(this.dataSource.getConnection(username, password));
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

  }

  @Override
  public void connectionErrorOccurred(ConnectionEvent connectionEvent) {

  }
}

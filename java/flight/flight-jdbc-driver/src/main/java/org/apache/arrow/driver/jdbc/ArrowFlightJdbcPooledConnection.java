package org.apache.arrow.driver.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

import org.apache.arrow.driver.jdbc.utils.ConnectionWrapper;

public class ArrowFlightJdbcPooledConnection implements PooledConnection {

  private final ArrowFlightConnection connection;
  private final List<ConnectionEventListener> eventListeners;
  private final List<StatementEventListener> statementEventListeners;

  private class InnerConnection extends ConnectionWrapper {
    public InnerConnection() {
      super(connection);
    }

    @Override
    public void close() throws SQLException {
      onConnectionClosed();
    }
  }

  public ArrowFlightJdbcPooledConnection(Connection connection) {
    this.connection = connection;
    this.eventListeners = Collections.synchronizedList(new ArrayList<>());
    this.statementEventListeners = Collections.synchronizedList(new ArrayList<>());
  }

  @Override
  public Connection getConnection() throws SQLException {
    return new InnerConnection();
  }

  @Override
  public void close() throws SQLException {
    this.connection.close();
  }

  @Override
  public void addConnectionEventListener(ConnectionEventListener listener) {
    if (!eventListeners.contains(listener)) {
      eventListeners.add(listener);
    }
  }

  @Override
  public void removeConnectionEventListener(ConnectionEventListener listener) {
    this.eventListeners.remove(listener);
  }

  @Override
  public void addStatementEventListener(StatementEventListener listener) {
    if (!statementEventListeners.contains(listener)) {
      statementEventListeners.add(listener);
    }
  }

  @Override
  public void removeStatementEventListener(StatementEventListener listener) {
    this.statementEventListeners.remove(listener);
  }

  private void onConnectionClosed() {
    ConnectionEvent connectionEvent = new ConnectionEvent(this);
    eventListeners.forEach(listener -> listener.connectionClosed(connectionEvent));
  }
}

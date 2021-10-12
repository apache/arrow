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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

import org.apache.arrow.driver.jdbc.utils.ConnectionWrapper;

/**
 * {@link PooledConnection} implementation for Arrow Flight JDBC Driver.
 */
public class ArrowFlightJdbcPooledConnection implements PooledConnection {

  private final ArrowFlightConnection connection;
  private final Set<ConnectionEventListener> eventListeners;
  private final Set<StatementEventListener> statementEventListeners;

  private final class ConnectionHandle extends ConnectionWrapper {
    private boolean closed = false;

    public ConnectionHandle() {
      super(connection);
    }

    @Override
    public void close() throws SQLException {
      if (!closed) {
        closed = true;
        onConnectionClosed();
      }
    }

    @Override
    public boolean isClosed() throws SQLException {
      return this.closed || super.isClosed();
    }
  }

  ArrowFlightJdbcPooledConnection(ArrowFlightConnection connection) {
    this.connection = connection;
    this.eventListeners = Collections.synchronizedSet(new HashSet<>());
    this.statementEventListeners = Collections.synchronizedSet(new HashSet<>());
  }

  public Properties getProperties() {
    return connection.getClientInfo();
  }

  @Override
  public Connection getConnection() throws SQLException {
    return new ConnectionHandle();
  }

  @Override
  public void close() throws SQLException {
    this.connection.close();
  }

  void reset() throws SQLException {
    this.connection.reset();
  }

  @Override
  public void addConnectionEventListener(ConnectionEventListener listener) {
    eventListeners.add(listener);
  }

  @Override
  public void removeConnectionEventListener(ConnectionEventListener listener) {
    this.eventListeners.remove(listener);
  }

  @Override
  public void addStatementEventListener(StatementEventListener listener) {
    statementEventListeners.add(listener);
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

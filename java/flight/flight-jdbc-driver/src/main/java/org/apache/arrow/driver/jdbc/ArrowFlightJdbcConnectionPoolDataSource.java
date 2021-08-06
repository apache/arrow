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
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

/**
 * {@link ConnectionPoolDataSource} implementation for Arrow Flight JDBC Driver.
 */
public class ArrowFlightJdbcConnectionPoolDataSource extends ArrowFlightJdbcDataSource
    implements ConnectionPoolDataSource, ConnectionEventListener, AutoCloseable {
  private final Map<Properties, Queue<PooledConnection>> pool = new ConcurrentHashMap<>();

  @Override
  public PooledConnection getPooledConnection() throws SQLException {
    return this.getPooledConnection(getUsername(), getPassword());
  }

  @Override
  public PooledConnection getPooledConnection(String username, String password) throws SQLException {
    Properties properties = this.getProperties(username, password);
    Queue<PooledConnection> objectPool = pool.computeIfAbsent(properties, s -> new ConcurrentLinkedQueue<>());
    PooledConnection pooledConnection = objectPool.poll();
    if (pooledConnection == null) {
      pooledConnection = createPooledConnection(properties);
    }
    return pooledConnection;
  }

  private PooledConnection createPooledConnection(Properties properties) throws SQLException {
    ArrowFlightJdbcPooledConnection pooledConnection =
        new ArrowFlightJdbcPooledConnection(this.getConnection(properties));
    pooledConnection.addConnectionEventListener(this);
    return pooledConnection;
  }

  @Override
  public void connectionClosed(ConnectionEvent connectionEvent) {
    ArrowFlightJdbcPooledConnection pooledConnection = (ArrowFlightJdbcPooledConnection) connectionEvent.getSource();
    Properties properties = pooledConnection.getProperties();

    this.pool.get(properties).add(pooledConnection);
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
}

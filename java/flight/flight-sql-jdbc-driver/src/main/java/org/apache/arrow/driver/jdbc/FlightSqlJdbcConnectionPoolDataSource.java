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

import org.apache.arrow.driver.jdbc.utils.FlightSqlConnectionConfigImpl;

/**
 * {@link ConnectionPoolDataSource} implementation for Arrow Flight SQL JDBC Driver.
 */
public class FlightSqlJdbcConnectionPoolDataSource extends FlightSqlJdbcDataSource
    implements ConnectionPoolDataSource, ConnectionEventListener, AutoCloseable {
  private final Map<Properties, Queue<FlightSqlJdbcPooledConnection>> pool =
      new ConcurrentHashMap<>();

  /**
   * Instantiates a new DataSource.
   *
   * @param properties the properties
   * @param config     the config.
   */
  protected FlightSqlJdbcConnectionPoolDataSource(final Properties properties,
                                                  final FlightSqlConnectionConfigImpl config) {
    super(properties, config);
  }

  /**
   * Creates a new {@link FlightSqlJdbcConnectionPoolDataSource}.
   *
   * @param properties the properties.
   * @return a new data source.
   */
  public static FlightSqlJdbcConnectionPoolDataSource createNewDataSource(
      final Properties properties) {
    return new FlightSqlJdbcConnectionPoolDataSource(properties,
        new FlightSqlConnectionConfigImpl(properties));
  }

  @Override
  public PooledConnection getPooledConnection() throws SQLException {
    final FlightSqlConnectionConfigImpl config = getConfig();
    return this.getPooledConnection(config.getUser(), config.getPassword());
  }

  @Override
  public PooledConnection getPooledConnection(final String username, final String password)
      throws SQLException {
    final Properties properties = getProperties(username, password);
    Queue<FlightSqlJdbcPooledConnection> objectPool =
        pool.computeIfAbsent(properties, s -> new ConcurrentLinkedQueue<>());
    FlightSqlJdbcPooledConnection pooledConnection = objectPool.poll();
    if (pooledConnection == null) {
      pooledConnection = createPooledConnection(new FlightSqlConnectionConfigImpl(properties));
    } else {
      pooledConnection.reset();
    }
    return pooledConnection;
  }

  private FlightSqlJdbcPooledConnection createPooledConnection(
      final FlightSqlConnectionConfigImpl config)
      throws SQLException {
    FlightSqlJdbcPooledConnection pooledConnection =
        new FlightSqlJdbcPooledConnection(getConnection(config.getUser(), config.getPassword()));
    pooledConnection.addConnectionEventListener(this);
    return pooledConnection;
  }

  @Override
  public void connectionClosed(ConnectionEvent connectionEvent) {
    final FlightSqlJdbcPooledConnection pooledConnection =
        (FlightSqlJdbcPooledConnection) connectionEvent.getSource();
    pool.get(pooledConnection.getProperties()).add(pooledConnection);
  }

  @Override
  public void connectionErrorOccurred(ConnectionEvent connectionEvent) {

  }

  @Override
  public void close() throws Exception {
    SQLException lastException = null;
    for (Queue<FlightSqlJdbcPooledConnection> connections : this.pool.values()) {
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

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

package org.apache.arrow.driver.jdbc.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcPooledConnection;

/**
 * Auxiliary wrapper class for {@link Connection}, used on {@link ArrowFlightJdbcPooledConnection}.
 */
public class ConnectionWrapper implements Connection {
  private final Connection realConnection;

  public ConnectionWrapper(final Connection connection) {
    realConnection = checkNotNull(connection);
  }

  @Override
  public final <T> T unwrap(final Class<T> type) {
    return type.cast(realConnection);
  }

  @Override
  public final boolean isWrapperFor(final Class<?> type) {
    return realConnection.getClass().isAssignableFrom(type);
  }

  @Override
  public final Statement createStatement() throws SQLException {
    return realConnection.createStatement();
  }

  @Override
  public final PreparedStatement prepareStatement(final String sqlQuery) throws SQLException {
    return realConnection.prepareStatement(sqlQuery);
  }

  @Override
  public final CallableStatement prepareCall(final String sqlQuery) throws SQLException {
    return realConnection.prepareCall(sqlQuery);
  }

  @Override
  public final String nativeSQL(final String sqlStatement) throws SQLException {
    return realConnection.nativeSQL(sqlStatement);
  }

  @Override
  public final void setAutoCommit(boolean autoCommit) throws SQLException {
    realConnection.setAutoCommit(autoCommit);
  }

  @Override
  public final boolean getAutoCommit() throws SQLException {
    return realConnection.getAutoCommit();
  }

  @Override
  public final void commit() throws SQLException {
    realConnection.commit();
  }

  @Override
  public final void rollback() throws SQLException {
    realConnection.rollback();
  }

  @Override
  public void close() throws SQLException {
    realConnection.close();
  }

  @Override
  public final boolean isClosed() throws SQLException {
    return realConnection.isClosed();
  }

  @Override
  public final DatabaseMetaData getMetaData() throws SQLException {
    return realConnection.getMetaData();
  }

  @Override
  public final void setReadOnly(final boolean readOnly) throws SQLException {
    realConnection.setReadOnly(readOnly);
  }

  @Override
  public final boolean isReadOnly() throws SQLException {
    return realConnection.isReadOnly();
  }

  @Override
  public final void setCatalog(final String catalogName) throws SQLException {
    realConnection.setCatalog(catalogName);
  }

  @Override
  public final String getCatalog() throws SQLException {
    return realConnection.getCatalog();
  }

  @Override
  public final void setTransactionIsolation(final int transactionIsolationId) throws SQLException {
    realConnection.setTransactionIsolation(transactionIsolationId);
  }

  @Override
  public final int getTransactionIsolation() throws SQLException {
    return realConnection.getTransactionIsolation();
  }

  @Override
  public final SQLWarning getWarnings() throws SQLException {
    return realConnection.getWarnings();
  }

  @Override
  public final void clearWarnings() throws SQLException {
    realConnection.clearWarnings();
  }

  @Override
  public final Statement createStatement(final int resultSetTypeId, final int resultSetConcurrencyId)
      throws SQLException {
    return realConnection.createStatement(resultSetTypeId, resultSetConcurrencyId);
  }

  @Override
  public final PreparedStatement prepareStatement(final String sqlQuery, final int resultSetTypeId,
                                                  final int resultSetConcurrencyId)
      throws SQLException {
    return realConnection.prepareStatement(sqlQuery, resultSetTypeId, resultSetConcurrencyId);
  }

  @Override
  public final CallableStatement prepareCall(final String query, final int resultSetTypeId,
                                             final int resultSetConcurrencyId)
      throws SQLException {
    return realConnection.prepareCall(query, resultSetTypeId, resultSetConcurrencyId);
  }

  @Override
  public final Map<String, Class<?>> getTypeMap() throws SQLException {
    return realConnection.getTypeMap();
  }

  @Override
  public final void setTypeMap(final Map<String, Class<?>> typeNameToClass) throws SQLException {
    realConnection.setTypeMap(typeNameToClass);
  }

  @Override
  public final void setHoldability(final int holdabilityId) throws SQLException {
    realConnection.setHoldability(holdabilityId);
  }

  @Override
  public final int getHoldability() throws SQLException {
    return realConnection.getHoldability();
  }

  @Override
  public final Savepoint setSavepoint() throws SQLException {
    return realConnection.setSavepoint();
  }

  @Override
  public final Savepoint setSavepoint(final String savepointName) throws SQLException {
    return realConnection.setSavepoint(savepointName);
  }

  @Override
  public final void rollback(final Savepoint savepoint) throws SQLException {
    realConnection.rollback(savepoint);
  }

  @Override
  public final void releaseSavepoint(final Savepoint savepoint) throws SQLException {
    realConnection.releaseSavepoint(savepoint);
  }

  @Override
  public final Statement createStatement(final int resultSetType,
                                         final int resultSetConcurrency,
                                         final int resultSetHoldability) throws SQLException {
    return realConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public final PreparedStatement prepareStatement(final String sqlQuery,
                                                  final int resultSetType,
                                                  final int resultSetConcurrency,
                                                  final int resultSetHoldability) throws SQLException {
    return realConnection.prepareStatement(sqlQuery, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public final CallableStatement prepareCall(final String sqlQuery,
                                             final int resultSetType,
                                             final int resultSetConcurrency,
                                             final int resultSetHoldability) throws SQLException {
    return realConnection.prepareCall(sqlQuery, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public final PreparedStatement prepareStatement(final String sqlQuery, final int autoGeneratedKeysId)
      throws SQLException {
    return realConnection.prepareStatement(sqlQuery, autoGeneratedKeysId);
  }

  @Override
  public final PreparedStatement prepareStatement(final String sqlQuery, final int... columnIndices)
      throws SQLException {
    return realConnection.prepareStatement(sqlQuery, columnIndices);
  }

  @Override
  public final PreparedStatement prepareStatement(final String sqlQuery, final String... columnNames)
      throws SQLException {
    return realConnection.prepareStatement(sqlQuery, columnNames);
  }

  @Override
  public final Clob createClob() throws SQLException {
    return realConnection.createClob();
  }

  @Override
  public final Blob createBlob() throws SQLException {
    return realConnection.createBlob();
  }

  @Override
  public final NClob createNClob() throws SQLException {
    return realConnection.createNClob();
  }

  @Override
  public final SQLXML createSQLXML() throws SQLException {
    return realConnection.createSQLXML();
  }

  @Override
  public final boolean isValid(final int timeout) throws SQLException {
    return realConnection.isValid(timeout);
  }

  @Override
  public final void setClientInfo(final String propertyName, final String propertyValue)
      throws SQLClientInfoException {
    realConnection.setClientInfo(propertyName, propertyValue);
  }

  @Override
  public final void setClientInfo(final Properties properties) throws SQLClientInfoException {
    realConnection.setClientInfo(properties);
  }

  @Override
  public final String getClientInfo(final String propertyName) throws SQLException {
    return realConnection.getClientInfo(propertyName);
  }

  @Override
  public final Properties getClientInfo() throws SQLException {
    return realConnection.getClientInfo();
  }

  @Override
  public final Array createArrayOf(final String typeName, final Object... elements) throws SQLException {
    return realConnection.createArrayOf(typeName, elements);
  }

  @Override
  public final Struct createStruct(final String typeName, final Object... attributes) throws SQLException {
    return realConnection.createStruct(typeName, attributes);
  }

  @Override
  public final void setSchema(final String schemaName) throws SQLException {
    realConnection.setSchema(schemaName);
  }

  @Override
  public final String getSchema() throws SQLException {
    return realConnection.getSchema();
  }

  @Override
  public final void abort(final Executor executor) throws SQLException {
    realConnection.abort(executor);
  }

  @Override
  public final void setNetworkTimeout(final Executor executor, final int timeoutInMillis) throws SQLException {
    realConnection.setNetworkTimeout(executor, timeoutInMillis);
  }

  @Override
  public final int getNetworkTimeout() throws SQLException {
    return realConnection.getNetworkTimeout();
  }
}

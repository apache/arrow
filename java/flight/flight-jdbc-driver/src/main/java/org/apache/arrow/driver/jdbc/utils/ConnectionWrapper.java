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

  public ConnectionWrapper(Connection connection) {
    realConnection = connection;
  }

  @Override
  public <T> T unwrap(Class<T> type) throws SQLException {
    return type.cast(realConnection);
  }

  @Override
  public boolean isWrapperFor(Class<?> type) throws SQLException {
    return realConnection.getClass().isAssignableFrom(type);
  }

  @Override
  public Statement createStatement() throws SQLException {
    return realConnection.createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String s) throws SQLException {
    return realConnection.prepareStatement(s);
  }

  @Override
  public CallableStatement prepareCall(String s) throws SQLException {
    return realConnection.prepareCall(s);
  }

  @Override
  public String nativeSQL(String s) throws SQLException {
    return realConnection.nativeSQL(s);
  }

  @Override
  public void setAutoCommit(boolean b) throws SQLException {
    realConnection.setAutoCommit(b);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return realConnection.getAutoCommit();
  }

  @Override
  public void commit() throws SQLException {
    realConnection.commit();
  }

  @Override
  public void rollback() throws SQLException {
    realConnection.rollback();
  }

  @Override
  public void close() throws SQLException {
    realConnection.close();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return realConnection.isClosed();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return realConnection.getMetaData();
  }

  @Override
  public void setReadOnly(boolean b) throws SQLException {
    realConnection.setReadOnly(b);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return realConnection.isReadOnly();
  }

  @Override
  public void setCatalog(String s) throws SQLException {
    realConnection.setCatalog(s);
  }

  @Override
  public String getCatalog() throws SQLException {
    return realConnection.getCatalog();
  }

  @Override
  public void setTransactionIsolation(int i) throws SQLException {
    realConnection.setTransactionIsolation(i);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return realConnection.getTransactionIsolation();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return realConnection.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    realConnection.clearWarnings();
  }

  @Override
  public Statement createStatement(int i, int i1) throws SQLException {
    return realConnection.createStatement(i, i1);
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i1) throws SQLException {
    return realConnection.prepareStatement(s, i, i1);
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i1) throws SQLException {
    return realConnection.prepareCall(s, i, i1);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return realConnection.getTypeMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    realConnection.setTypeMap(map);
  }

  @Override
  public void setHoldability(int i) throws SQLException {
    realConnection.setHoldability(i);
  }

  @Override
  public int getHoldability() throws SQLException {
    return realConnection.getHoldability();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return realConnection.setSavepoint();
  }

  @Override
  public Savepoint setSavepoint(String s) throws SQLException {
    return realConnection.setSavepoint(s);
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    realConnection.rollback(savepoint);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    realConnection.releaseSavepoint(savepoint);
  }

  @Override
  public Statement createStatement(int i, int i1, int i2) throws SQLException {
    return realConnection.createStatement(i, i1, i2);
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i1, int i2) throws SQLException {
    return realConnection.prepareStatement(s, i, i1, i2);
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i1, int i2) throws SQLException {
    return realConnection.prepareCall(s, i, i1, i2);
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i) throws SQLException {
    return realConnection.prepareStatement(s, i);
  }

  @Override
  public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
    return realConnection.prepareStatement(s, ints);
  }

  @Override
  public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
    return realConnection.prepareStatement(s, strings);
  }

  @Override
  public Clob createClob() throws SQLException {
    return realConnection.createClob();
  }

  @Override
  public Blob createBlob() throws SQLException {
    return realConnection.createBlob();
  }

  @Override
  public NClob createNClob() throws SQLException {
    return realConnection.createNClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return realConnection.createSQLXML();
  }

  @Override
  public boolean isValid(int i) throws SQLException {
    return realConnection.isValid(i);
  }

  @Override
  public void setClientInfo(String s, String s1) throws SQLClientInfoException {
    realConnection.setClientInfo(s, s1);
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    realConnection.setClientInfo(properties);
  }

  @Override
  public String getClientInfo(String s) throws SQLException {
    return realConnection.getClientInfo(s);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return realConnection.getClientInfo();
  }

  @Override
  public Array createArrayOf(String s, Object[] objects) throws SQLException {
    return realConnection.createArrayOf(s, objects);
  }

  @Override
  public Struct createStruct(String s, Object[] objects) throws SQLException {
    return realConnection.createStruct(s, objects);
  }

  @Override
  public void setSchema(String s) throws SQLException {
    realConnection.setSchema(s);
  }

  @Override
  public String getSchema() throws SQLException {
    return realConnection.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    realConnection.abort(executor);
  }

  @Override
  public void setNetworkTimeout(Executor executor, int i) throws SQLException {
    realConnection.setNetworkTimeout(executor, i);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return realConnection.getNetworkTimeout();
  }
}

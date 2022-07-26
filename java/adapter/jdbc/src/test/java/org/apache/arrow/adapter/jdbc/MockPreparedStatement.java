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

package org.apache.arrow.adapter.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/** A PreparedStatement that just stores parameters set on it. */
public final class MockPreparedStatement implements PreparedStatement {
  static class ParameterHolder {
    final Object value;
    final Integer sqlType;
    public Calendar calendar;

    ParameterHolder(Object value, Integer sqlType) {
      this.value = value;
      this.sqlType = sqlType;
    }
  }

  private final Map<Integer, ParameterHolder> parameters;

  MockPreparedStatement() {
    parameters = new HashMap<>();
  }

  ParameterHolder getParam(int index) {
    return parameters.get(index);
  }

  Object getParamValue(int index) {
    return parameters.get(index).value;
  }

  Integer getParamType(int index) {
    return parameters.get(index).sqlType;
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(null, sqlType));
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  @Deprecated
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void clearParameters() throws SQLException {
    parameters.clear();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, targetSqlType));
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public boolean execute() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(reader, null));
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {}

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    ParameterHolder value = new ParameterHolder(x, null);
    value.calendar = cal;
    parameters.put(parameterIndex, value);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {}

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    parameters.put(parameterIndex, new ParameterHolder(x, null));
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {}

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {}

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {}

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {}

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {}

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {}

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {}

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {}

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {}

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {}

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {}

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {}

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {}

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {}

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {}

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {}

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {}

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {}

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws SQLException {}

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMaxRows() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cancel() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getResultSetType() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Connection getConnection() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPoolable() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }
}

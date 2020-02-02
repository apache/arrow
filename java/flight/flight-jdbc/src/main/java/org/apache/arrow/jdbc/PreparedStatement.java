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

package org.apache.arrow.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * PreparedStatement.
 */
public class PreparedStatement extends Statement implements java.sql.PreparedStatement {

  @Override
  public ResultSet executeQuery() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNull(int i, int i1) throws SQLException {

  }

  @Override
  public void setBoolean(int i, boolean b) throws SQLException {

  }

  @Override
  public void setByte(int i, byte b) throws SQLException {

  }

  @Override
  public void setShort(int i, short i1) throws SQLException {

  }

  @Override
  public void setInt(int i, int i1) throws SQLException {

  }

  @Override
  public void setLong(int i, long l) throws SQLException {

  }

  @Override
  public void setFloat(int i, float v) throws SQLException {

  }

  @Override
  public void setDouble(int i, double v) throws SQLException {

  }

  @Override
  public void setBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {

  }

  @Override
  public void setString(int i, String s) throws SQLException {

  }

  @Override
  public void setBytes(int i, byte[] bytes) throws SQLException {

  }

  @Override
  public void setDate(int i, Date date) throws SQLException {

  }

  @Override
  public void setTime(int i, Time time) throws SQLException {

  }

  @Override
  public void setTimestamp(int i, Timestamp timestamp) throws SQLException {

  }

  @Override
  public void setAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {

  }

  @Override
  public void setUnicodeStream(int i, InputStream inputStream, int i1) throws SQLException {

  }

  @Override
  public void setBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {

  }

  @Override
  public void clearParameters() throws SQLException {

  }

  @Override
  public void setObject(int i, Object o, int i1) throws SQLException {

  }

  @Override
  public void setObject(int i, Object o) throws SQLException {

  }

  @Override
  public boolean execute() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void addBatch() throws SQLException {

  }

  @Override
  public void setCharacterStream(int i, Reader reader, int i1) throws SQLException {

  }

  @Override
  public void setRef(int i, Ref ref) throws SQLException {

  }

  @Override
  public void setBlob(int i, Blob blob) throws SQLException {

  }

  @Override
  public void setClob(int i, Clob clob) throws SQLException {

  }

  @Override
  public void setArray(int i, Array array) throws SQLException {

  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setDate(int i, Date date, Calendar calendar) throws SQLException {

  }

  @Override
  public void setTime(int i, Time time, Calendar calendar) throws SQLException {

  }

  @Override
  public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {

  }

  @Override
  public void setNull(int i, int i1, String s) throws SQLException {

  }

  @Override
  public void setURL(int i, URL url) throws SQLException {

  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setRowId(int i, RowId rowId) throws SQLException {

  }

  @Override
  public void setNString(int i, String s) throws SQLException {

  }

  @Override
  public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {

  }

  @Override
  public void setNClob(int i, NClob nClob) throws SQLException {

  }

  @Override
  public void setClob(int i, Reader reader, long l) throws SQLException {

  }

  @Override
  public void setBlob(int i, InputStream inputStream, long l) throws SQLException {

  }

  @Override
  public void setNClob(int i, Reader reader, long l) throws SQLException {

  }

  @Override
  public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {

  }

  @Override
  public void setObject(int i, Object o, int i1, int i2) throws SQLException {

  }

  @Override
  public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {

  }

  @Override
  public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {

  }

  @Override
  public void setCharacterStream(int i, Reader reader, long l) throws SQLException {

  }

  @Override
  public void setAsciiStream(int i, InputStream inputStream) throws SQLException {

  }

  @Override
  public void setBinaryStream(int i, InputStream inputStream) throws SQLException {

  }

  @Override
  public void setCharacterStream(int i, Reader reader) throws SQLException {

  }

  @Override
  public void setNCharacterStream(int i, Reader reader) throws SQLException {

  }

  @Override
  public void setClob(int i, Reader reader) throws SQLException {

  }

  @Override
  public void setBlob(int i, InputStream inputStream) throws SQLException {

  }

  @Override
  public void setNClob(int i, Reader reader) throws SQLException {

  }
}

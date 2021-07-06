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

package org.apache.arrow.driver.jdbc.accessor;

import static org.apache.arrow.driver.jdbc.utils.ExceptionTemplateThrower.getOperationNotSupported;
import static org.apache.calcite.avatica.util.Cursor.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * Base Jdbc Accessor.
 */
public abstract class ArrowFlightJdbcAccessor implements Accessor {
  @Override
  public boolean wasNull() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public String getString() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public boolean getBoolean() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public byte getByte() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public short getShort() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public int getInt() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public long getLong() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public float getFloat() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public double getDouble() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public BigDecimal getBigDecimal() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public BigDecimal getBigDecimal(int i) throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public byte[] getBytes() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public InputStream getAsciiStream() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public InputStream getUnicodeStream() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public InputStream getBinaryStream() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Object getObject() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Object getObject(Map<String, Class<?>> map) throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Ref getRef() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Blob getBlob() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Clob getClob() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Array getArray() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Struct getStruct() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Date getDate(Calendar calendar) throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Time getTime(Calendar calendar) throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public URL getURL() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public NClob getNClob() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public SQLXML getSQLXML() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public String getNString() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Reader getNCharacterStream() throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public <T> T getObject(Class<T> aClass) throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }
}

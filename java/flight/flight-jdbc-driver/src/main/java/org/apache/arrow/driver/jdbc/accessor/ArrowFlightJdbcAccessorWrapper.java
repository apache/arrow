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
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.function.IntSupplier;

/**
 * Abstract accessor wrapper, used for complex types accessors to leverage other types accessors.
 */
public abstract class ArrowFlightJdbcAccessorWrapper extends ArrowFlightJdbcAccessor {

  protected ArrowFlightJdbcAccessorWrapper(IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
  }

  protected abstract ArrowFlightJdbcAccessor getAccessor();

  @Override
  public Class<?> getObjectClass() {
    return getAccessor().getObjectClass();
  }

  @Override
  public boolean wasNull() {
    return getAccessor().wasNull();
  }

  @Override
  public String getString() {
    return getAccessor().getString();
  }

  @Override
  public boolean getBoolean() {
    return getAccessor().getBoolean();
  }

  @Override
  public byte getByte() {
    return getAccessor().getByte();
  }

  @Override
  public short getShort() {
    return getAccessor().getShort();
  }

  @Override
  public int getInt() {
    return getAccessor().getInt();
  }

  @Override
  public long getLong() {
    return getAccessor().getLong();
  }

  @Override
  public float getFloat() {
    return getAccessor().getFloat();
  }

  @Override
  public double getDouble() {
    return getAccessor().getDouble();
  }

  @Override
  public BigDecimal getBigDecimal() {
    return getAccessor().getBigDecimal();
  }

  @Override
  public BigDecimal getBigDecimal(int i) {
    return getAccessor().getBigDecimal(i);
  }

  @Override
  public byte[] getBytes() {
    return getAccessor().getBytes();
  }

  @Override
  public InputStream getAsciiStream() {
    return getAccessor().getAsciiStream();
  }

  @Override
  public InputStream getUnicodeStream() {
    return getAccessor().getUnicodeStream();
  }

  @Override
  public InputStream getBinaryStream() {
    return getAccessor().getBinaryStream();
  }

  @Override
  public Object getObject() {
    return getAccessor().getObject();
  }

  @Override
  public Reader getCharacterStream() {
    return getAccessor().getCharacterStream();
  }

  @Override
  public Object getObject(Map<String, Class<?>> map) {
    return getAccessor().getObject(map);
  }

  @Override
  public Ref getRef() {
    return getAccessor().getRef();
  }

  @Override
  public Blob getBlob() {
    return getAccessor().getBlob();
  }

  @Override
  public Clob getClob() {
    return getAccessor().getClob();
  }

  @Override
  public Array getArray() {
    return getAccessor().getArray();
  }

  @Override
  public Struct getStruct() {
    return getAccessor().getStruct();
  }

  @Override
  public Date getDate(Calendar calendar) {
    return getAccessor().getDate(calendar);
  }

  @Override
  public Time getTime(Calendar calendar) {
    return getAccessor().getTime(calendar);
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) {
    return getAccessor().getTimestamp(calendar);
  }

  @Override
  public URL getURL() {
    return getAccessor().getURL();
  }

  @Override
  public NClob getNClob() {
    return getAccessor().getNClob();
  }

  @Override
  public SQLXML getSQLXML() {
    return getAccessor().getSQLXML();
  }

  @Override
  public String getNString() {
    return getAccessor().getNString();
  }

  @Override
  public Reader getNCharacterStream() {
    return getAccessor().getNCharacterStream();
  }

  @Override
  public <T> T getObject(Class<T> type) {
    return getAccessor().getObject(type);
  }
}

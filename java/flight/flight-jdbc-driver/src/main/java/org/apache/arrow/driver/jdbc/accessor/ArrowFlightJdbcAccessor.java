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
import static org.apache.calcite.avatica.util.Cursor.Accessor;

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

/**
 * Base Jdbc Accessor.
 */
public abstract class ArrowFlightJdbcAccessor implements Accessor {
  @Override
  public boolean wasNull() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public String getString() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public boolean getBoolean() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public byte getByte() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public short getShort() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public int getInt() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public long getLong() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public float getFloat() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public double getDouble() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public BigDecimal getBigDecimal() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public BigDecimal getBigDecimal(int i) {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public byte[] getBytes() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public InputStream getAsciiStream() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public InputStream getUnicodeStream() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public InputStream getBinaryStream() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Object getObject() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Reader getCharacterStream() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Object getObject(Map<String, Class<?>> map) {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Ref getRef() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Blob getBlob() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Clob getClob() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Array getArray() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Struct getStruct() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Date getDate(Calendar calendar) {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Time getTime(Calendar calendar) {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public URL getURL() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public NClob getNClob() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public SQLXML getSQLXML() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public String getNString() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Reader getNCharacterStream() {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public <T> T getObject(Class<T> aClass) {
    if (aClass.isAssignableFrom(long.class)) {
      return aClass.cast(getLong());
    } else if (aClass == int.class) {
      return aClass.cast(getInt());
    } else if (aClass == short.class) {
      return aClass.cast(getShort());
    } else if (aClass == byte.class) {
      return aClass.cast(getByte());
    } else if (aClass == String.class) {
      return aClass.cast(getString());
    } else if (aClass == float.class) {
      return aClass.cast(getFloat());
    } else if (aClass == double.class) {
      return aClass.cast(getDouble());
    } else if (aClass == byte[].class) {
      return aClass.cast(getBytes());
    }

    throw new UnsupportedOperationException();
  }
}

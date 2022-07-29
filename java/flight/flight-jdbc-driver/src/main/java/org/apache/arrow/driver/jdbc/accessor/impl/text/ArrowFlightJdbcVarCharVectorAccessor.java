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

package org.apache.arrow.driver.jdbc.accessor.impl.text;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.DateTimeUtils;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;

/**
 * Accessor for the Arrow types: {@link VarCharVector} and {@link LargeVarCharVector}.
 */
public class ArrowFlightJdbcVarCharVectorAccessor extends ArrowFlightJdbcAccessor {

  /**
   * Functional interface to help integrating VarCharVector and LargeVarCharVector.
   */
  @FunctionalInterface
  interface Getter {
    byte[] get(int index);
  }

  private final Getter getter;

  public ArrowFlightJdbcVarCharVectorAccessor(VarCharVector vector,
                                              IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector::get, currentRowSupplier, setCursorWasNull);
  }

  public ArrowFlightJdbcVarCharVectorAccessor(LargeVarCharVector vector,
                                              IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector::get, currentRowSupplier, setCursorWasNull);
  }

  ArrowFlightJdbcVarCharVectorAccessor(Getter getter,
                                       IntSupplier currentRowSupplier,
                                       ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.getter = getter;
  }

  @Override
  public Class<?> getObjectClass() {
    return String.class;
  }

  @Override
  public String getObject() {
    final byte[] bytes = getBytes();
    return bytes == null ? null : new String(bytes, UTF_8);
  }

  @Override
  public String getString() {
    return getObject();
  }

  @Override
  public byte[] getBytes() {
    final byte[] bytes = this.getter.get(getCurrentRow());
    this.wasNull = bytes == null;
    this.wasNullConsumer.setWasNull(this.wasNull);
    return bytes;
  }

  @Override
  public boolean getBoolean() throws SQLException {
    String value = getString();
    if (value == null || value.equalsIgnoreCase("false") || value.equals("0")) {
      return false;
    } else if (value.equalsIgnoreCase("true") || value.equals("1")) {
      return true;
    } else {
      throw new SQLException("It is not possible to convert this value to boolean: " + value);
    }
  }

  @Override
  public byte getByte() throws SQLException {
    try {
      return Byte.parseByte(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public short getShort() throws SQLException {
    try {
      return Short.parseShort(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public int getInt() throws SQLException {
    try {
      return Integer.parseInt(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public long getLong() throws SQLException {
    try {
      return Long.parseLong(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public float getFloat() throws SQLException {
    try {
      return Float.parseFloat(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public double getDouble() throws SQLException {
    try {
      return Double.parseDouble(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public BigDecimal getBigDecimal() throws SQLException {
    try {
      return new BigDecimal(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public BigDecimal getBigDecimal(int i) throws SQLException {
    try {
      return BigDecimal.valueOf(this.getLong(), i);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public InputStream getAsciiStream() {
    final String textValue = getString();
    if (textValue == null) {
      return null;
    }
    // Already in UTF-8
    return new ByteArrayInputStream(textValue.getBytes(US_ASCII));
  }

  @Override
  public InputStream getUnicodeStream() {
    final byte[] value = getBytes();
    if (value == null) {
      return null;
    }

    // Already in UTF-8
    final Text textValue = new Text(value);
    return new ByteArrayInputStream(textValue.getBytes(), 0, textValue.getLength());
  }

  @Override
  public Reader getCharacterStream() {
    return new CharArrayReader(getString().toCharArray());
  }

  @Override
  public Date getDate(Calendar calendar) throws SQLException {
    try {
      Date date = Date.valueOf(getString());
      if (calendar == null) {
        return date;
      }

      // Use Calendar to apply time zone's offset
      long milliseconds = date.getTime();
      return new Date(DateTimeUtils.applyCalendarOffset(milliseconds, calendar));
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public Time getTime(Calendar calendar) throws SQLException {
    try {
      Time time = Time.valueOf(getString());
      if (calendar == null) {
        return time;
      }

      // Use Calendar to apply time zone's offset
      long milliseconds = time.getTime();
      return new Time(DateTimeUtils.applyCalendarOffset(milliseconds, calendar));
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) throws SQLException {
    try {
      Timestamp timestamp = Timestamp.valueOf(getString());
      if (calendar == null) {
        return timestamp;
      }

      // Use Calendar to apply time zone's offset
      long milliseconds = timestamp.getTime();
      return new Timestamp(DateTimeUtils.applyCalendarOffset(milliseconds, calendar));
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }
}

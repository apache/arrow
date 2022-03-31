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
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.function.IntSupplier;
import org.apache.arrow.vector.util.DateUtility;

/**
 * Base Jdbc Accessor.
 */
public abstract class ArrowFlightJdbcAccessor implements Accessor {
  private final IntSupplier currentRowSupplier;

  // All the derived accessor classes should alter this as they encounter null Values
  protected boolean wasNull;
  protected ArrowFlightJdbcAccessorFactory.WasNullConsumer wasNullConsumer;

  protected ArrowFlightJdbcAccessor(final IntSupplier currentRowSupplier,
                                    ArrowFlightJdbcAccessorFactory.WasNullConsumer wasNullConsumer) {
    this.currentRowSupplier = currentRowSupplier;
    this.wasNullConsumer = wasNullConsumer;
  }

  protected int getCurrentRow() {
    return currentRowSupplier.getAsInt();
  }

  // It needs to be public so this method can be accessed when creating the complex types.
  public abstract Class<?> getObjectClass();

  @Override
  public boolean wasNull() {
    return wasNull;
  }

  @Override
  public String getString() throws SQLException {
    final Object object = getObject();
    if (object == null) {
      return null;
    }

    return object.toString();
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
  public BigDecimal getBigDecimal(final int i) throws SQLException {
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
  public Object getObject(final Map<String, Class<?>> map) throws SQLException {
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
  public Date getDate(final Calendar calendar) throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Time getTime(final Calendar calendar) throws SQLException {
    throw getOperationNotSupported(this.getClass());
  }

  @Override
  public Timestamp getTimestamp(final Calendar calendar) throws SQLException {
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
  public <T> T getObject(final Class<T> type) throws SQLException {
    final Object value;
    if (type == Byte.class) {
      value = getByte();
    } else if (type == Short.class) {
      value = getShort();
    } else if (type == Integer.class) {
      value = getInt();
    } else if (type == Long.class) {
      value = getLong();
    } else if (type == Float.class) {
      value = getFloat();
    } else if (type == Double.class) {
      value = getDouble();
    } else if (type == Boolean.class) {
      value = getBoolean();
    } else if (type == BigDecimal.class) {
      value = getBigDecimal();
    } else if (type == String.class) {
      value = getString();
    } else if (type == byte[].class) {
      value = getBytes();
    } else {
      value = getObject();
    }
    return !type.isPrimitive() && wasNull ? null : type.cast(value);
  }

  /**
   * Formats a period similar to Oracle INTERVAL YEAR TO MONTH data type<br>.
   * For example, the string "+21-02" defines an interval of 21 years and 2 months.
   */
  public static String formatIntervalYear(final org.joda.time.Period p) {
    long months = p.getYears() * (long) DateUtility.yearsToMonths + p.getMonths();
    boolean neg = false;
    if (months < 0) {
      months = -months;
      neg = true;
    }
    final int years = (int) (months / DateUtility.yearsToMonths);
    months = months % DateUtility.yearsToMonths;

    return String.format("%c%03d-%02d", neg ? '-' : '+', years, months);
  }

  /**
   * Formats a period similar to Oracle INTERVAL DAY TO SECOND data type.<br>.
   * For example, the string "-001 18:25:16.766" defines an interval of
   * - 1 day 18 hours 25 minutes 16 seconds and 766 milliseconds.
   */
  public static String formatIntervalDay(final org.joda.time.Period p) {
    long millis = p.getDays() * (long) DateUtility.daysToStandardMillis + millisFromPeriod(p);

    boolean neg = false;
    if (millis < 0) {
      millis = -millis;
      neg = true;
    }

    final int days = (int) (millis / DateUtility.daysToStandardMillis);
    millis = millis % DateUtility.daysToStandardMillis;

    final int hours = (int) (millis / DateUtility.hoursToMillis);
    millis = millis % DateUtility.hoursToMillis;

    final int minutes = (int) (millis / DateUtility.minutesToMillis);
    millis = millis % DateUtility.minutesToMillis;

    final int seconds = (int) (millis / DateUtility.secondsToMillis);
    millis = millis % DateUtility.secondsToMillis;

    return String.format("%c%03d %02d:%02d:%02d.%03d", neg ? '-' : '+', days, hours, minutes, seconds, millis);
  }

  public static int millisFromPeriod(org.joda.time.Period period) {
    return period.getHours() * DateUtility.hoursToMillis + period.getMinutes() * DateUtility.minutesToMillis +
        period.getSeconds() * DateUtility.secondsToMillis + period.getMillis();
  }

  private static SQLException getOperationNotSupported(final Class<?> type) {
    return new SQLException(String.format("Operation not supported for type: %s.", type.getName()));
  }
}

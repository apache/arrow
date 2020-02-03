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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Helper methods for converting between data types.
 */
public class ResultSetHelper {

  /** Convert value to String. */
  public static String getString(final Object value) throws SQLException {
    return value == null ? null : String.valueOf(value);
  }

  /** Convert value to boolean. */
  public static boolean getBoolean(final Object value) throws SQLException {
    if (value == null) {
      return false;
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      return ((String) value).equalsIgnoreCase("true");
    } else {
      throw unsupportedConversion("boolean", value);
    }
  }

  /** Convert value to byte. */
  public static byte getByte(final Object value) throws SQLException {
    if (value == null) {
      return 0;
    } else if (value instanceof Number) {
      return ((Number) value).byteValue();
    } else if (value instanceof String) {
      return Byte.parseByte((String) value);
    } else {
      throw unsupportedConversion("byte", value);
    }
  }

  /** Convert value to short. */
  public static short getShort(final Object value) throws SQLException {
    if (value == null) {
      return 0;
    } else if (value instanceof Number) {
      return ((Number) value).shortValue();
    } else if (value instanceof String) {
      return Short.parseShort((String) value);
    } else {
      throw unsupportedConversion("short", value);
    }
  }

  /** Convert value to int. */
  public static int getInt(final Object value) throws SQLException {
    if (value == null) {
      return 0;
    } else if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      return Integer.parseInt((String) value);
    } else {
      throw unsupportedConversion("int", value);
    }
  }

  /** Convert value to String. */
  public static long getLong(final Object value) throws SQLException {
    if (value == null) {
      return 0;
    } else if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      return Long.parseLong((String) value);
    } else {
      throw unsupportedConversion("long", value);
    }
  }

  /** Convert value to float. */
  public static float getFloat(final Object value) throws SQLException {
    if (value == null) {
      return 0;
    } else if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      return Float.parseFloat((String) value);
    } else {
      throw unsupportedConversion("float", value);
    }
  }

  /** Convert value to double. */
  public static double getDouble(final Object value) throws SQLException {
    if (value == null) {
      return 0;
    } else if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else if (value instanceof String) {
      return Double.parseDouble((String) value);
    } else {
      throw unsupportedConversion("double", value);
    }
  }

  /** Convert value to BigDecimal. */
  public static BigDecimal getBigDecimal(final Object value) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /** Convert value to byte[]. */
  public static byte[] getBytes(final Object value) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /** Convert value to Date. */
  public static Date getDate(final Object value) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /** Convert value to Time. */
  public static Time getTime(final Object value) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /** Convert value to Timestamp. */
  public static Timestamp getTimestamp(final Object value) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /** Convenience method for building an exception for unsupported conversions. */
  private static SQLException unsupportedConversion(String t, Object value) {
    if (value == null) {
      return new SQLException(String.format("Cannot convert null value to type %s", t));
    } else {
      return new SQLException(String.format("Cannot convert %s value '%s' to type %s", value.getClass(), value, t));
    }
  }
}

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

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
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
    Text get(int index);
  }

  private final Getter getter;

  public ArrowFlightJdbcVarCharVectorAccessor(VarCharVector vector,
                                              IntSupplier currentRowSupplier) {
    this(vector::getObject, currentRowSupplier);
  }

  public ArrowFlightJdbcVarCharVectorAccessor(LargeVarCharVector vector,
                                              IntSupplier currentRowSupplier) {
    this(vector::getObject, currentRowSupplier);
  }

  ArrowFlightJdbcVarCharVectorAccessor(Getter getter,
                                       IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.getter = getter;
  }

  @Override
  public Class<?> getObjectClass() {
    return Text.class;
  }

  private Text getText() {
    final Text text = this.getter.get(getCurrentRow());
    this.wasNull = text == null;
    return text;
  }

  @Override
  public String getObject() {
    final Text text = getText();
    return text == null ? null : text.toString();
  }

  @Override
  public String getString() {
    return getObject();
  }

  @Override
  public byte[] getBytes() {
    return this.getText().copyBytes();
  }

  @Override
  public boolean getBoolean() {
    String value = getString();
    return value != null && !value.isEmpty() && !value.equals("false") && !value.equals("0");
  }

  @Override
  public byte getByte() {
    return Byte.parseByte(this.getString());
  }

  @Override
  public short getShort() {
    return Short.parseShort(this.getString());
  }

  @Override
  public int getInt() {
    return Integer.parseInt(this.getString());
  }

  @Override
  public long getLong() {
    return Long.parseLong(this.getString());
  }

  @Override
  public float getFloat() {
    return Float.parseFloat(this.getString());
  }

  @Override
  public double getDouble() {
    return Double.parseDouble(this.getString());
  }

  @Override
  public BigDecimal getBigDecimal() {
    return new BigDecimal(this.getString());
  }

  @Override
  public BigDecimal getBigDecimal(int i) {
    return BigDecimal.valueOf(this.getLong(), i);
  }

  @Override
  public InputStream getAsciiStream() {
    Text value = this.getText();
    return new ByteArrayInputStream(value.getBytes(), 0, value.getLength());
  }

  @Override
  public Reader getCharacterStream() {
    return new CharArrayReader(getString().toCharArray());
  }

  @Override
  public Date getDate(Calendar calendar) {
    Date date = Date.valueOf(getString());
    if (calendar == null) {
      return date;
    }

    // Use Calendar to apply time zone's offset
    long milliseconds = date.getTime();
    return new Date(DateTimeUtils.applyCalendarOffset(milliseconds, calendar));
  }

  @Override
  public Time getTime(Calendar calendar) {
    Time time = Time.valueOf(getString());
    if (calendar == null) {
      return time;
    }

    // Use Calendar to apply time zone's offset
    long milliseconds = time.getTime();
    return new Time(DateTimeUtils.applyCalendarOffset(milliseconds, calendar));
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) {
    Timestamp timestamp = Timestamp.valueOf(getString());
    if (calendar == null) {
      return timestamp;
    }

    // Use Calendar to apply time zone's offset
    long milliseconds = timestamp.getTime();
    return new Timestamp(DateTimeUtils.applyCalendarOffset(milliseconds, calendar));
  }
}

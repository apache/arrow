/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import io.netty.buffer.ArrowBuf;

import java.math.BigDecimal;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.DateHolder;
import org.apache.arrow.vector.holders.Decimal18Holder;
import org.apache.arrow.vector.holders.Decimal28SparseHolder;
import org.apache.arrow.vector.holders.Decimal38SparseHolder;
import org.apache.arrow.vector.holders.Decimal9Holder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.IntervalDayHolder;
import org.apache.arrow.vector.holders.IntervalYearHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.TimeHolder;
import org.apache.arrow.vector.holders.TimeStampHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.util.DecimalUtility;

import com.google.common.base.Charsets;


public class ValueHolderHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueHolderHelper.class);

  public static IntHolder getIntHolder(int value) {
    IntHolder holder = new IntHolder();
    holder.value = value;

    return holder;
  }

  public static BigIntHolder getBigIntHolder(long value) {
    BigIntHolder holder = new BigIntHolder();
    holder.value = value;

    return holder;
  }

  public static Float4Holder getFloat4Holder(float value) {
    Float4Holder holder = new Float4Holder();
    holder.value = value;

    return holder;
  }

  public static Float8Holder getFloat8Holder(double value) {
    Float8Holder holder = new Float8Holder();
    holder.value = value;

    return holder;
  }

  public static DateHolder getDateHolder(long value) {
    DateHolder holder = new DateHolder();
    holder.value = value;
    return holder;
  }

  public static TimeHolder getTimeHolder(int value) {
    TimeHolder holder = new TimeHolder();
    holder.value = value;
    return holder;
  }

  public static TimeStampHolder getTimeStampHolder(long value) {
    TimeStampHolder holder = new TimeStampHolder();
    holder.value = value;
    return holder;
  }

  public static BitHolder getBitHolder(int value) {
    BitHolder holder = new BitHolder();
    holder.value = value;

    return holder;
  }

  public static NullableBitHolder getNullableBitHolder(boolean isNull, int value) {
    NullableBitHolder holder = new NullableBitHolder();
    holder.isSet = isNull? 0 : 1;
    if (! isNull) {
      holder.value = value;
    }

    return holder;
  }

  public static VarCharHolder getVarCharHolder(ArrowBuf buf, String s){
    VarCharHolder vch = new VarCharHolder();

    byte[] b = s.getBytes(Charsets.UTF_8);
    vch.start = 0;
    vch.end = b.length;
    vch.buffer = buf.reallocIfNeeded(b.length);
    vch.buffer.setBytes(0, b);
    return vch;
  }

  public static VarCharHolder getVarCharHolder(BufferAllocator a, String s){
    VarCharHolder vch = new VarCharHolder();

    byte[] b = s.getBytes(Charsets.UTF_8);
    vch.start = 0;
    vch.end = b.length;
    vch.buffer = a.buffer(b.length); //
    vch.buffer.setBytes(0, b);
    return vch;
  }


  public static IntervalYearHolder getIntervalYearHolder(int intervalYear) {
    IntervalYearHolder holder = new IntervalYearHolder();

    holder.value = intervalYear;
    return holder;
  }

  public static IntervalDayHolder getIntervalDayHolder(int days, int millis) {
      IntervalDayHolder dch = new IntervalDayHolder();

      dch.days = days;
      dch.milliseconds = millis;
      return dch;
  }

  public static Decimal9Holder getDecimal9Holder(int decimal, int scale, int precision) {
    Decimal9Holder dch = new Decimal9Holder();

    dch.scale = scale;
    dch.precision = precision;
    dch.value = decimal;

    return dch;
  }

  public static Decimal18Holder getDecimal18Holder(long decimal, int scale, int precision) {
    Decimal18Holder dch = new Decimal18Holder();

    dch.scale = scale;
    dch.precision = precision;
    dch.value = decimal;

    return dch;
  }

  public static Decimal28SparseHolder getDecimal28Holder(ArrowBuf buf, String decimal) {

    Decimal28SparseHolder dch = new Decimal28SparseHolder();

    BigDecimal bigDecimal = new BigDecimal(decimal);

    dch.scale = bigDecimal.scale();
    dch.precision = bigDecimal.precision();
    Decimal28SparseHolder.setSign(bigDecimal.signum() == -1, dch.start, dch.buffer);
    dch.start = 0;
    dch.buffer = buf.reallocIfNeeded(5 * DecimalUtility.INTEGER_SIZE);
    DecimalUtility
        .getSparseFromBigDecimal(bigDecimal, dch.buffer, dch.start, dch.scale, dch.precision, dch.nDecimalDigits);

    return dch;
  }

  public static Decimal38SparseHolder getDecimal38Holder(ArrowBuf buf, String decimal) {

      Decimal38SparseHolder dch = new Decimal38SparseHolder();

      BigDecimal bigDecimal = new BigDecimal(decimal);

      dch.scale = bigDecimal.scale();
      dch.precision = bigDecimal.precision();
      Decimal38SparseHolder.setSign(bigDecimal.signum() == -1, dch.start, dch.buffer);
      dch.start = 0;
    dch.buffer = buf.reallocIfNeeded(dch.maxPrecision * DecimalUtility.INTEGER_SIZE);
    DecimalUtility
        .getSparseFromBigDecimal(bigDecimal, dch.buffer, dch.start, dch.scale, dch.precision, dch.nDecimalDigits);

      return dch;
  }
}

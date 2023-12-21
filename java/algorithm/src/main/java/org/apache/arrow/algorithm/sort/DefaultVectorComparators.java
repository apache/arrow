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

package org.apache.arrow.algorithm.sort;

import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;

import java.math.BigDecimal;
import java.time.Duration;

import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.RepeatedValueVector;
import org.apache.arrow.vector.holders.NullableFixedSizeBinaryHolder;

/**
 * Default comparator implementations for different types of vectors.
 */
public class DefaultVectorComparators {

  /**
   * Create the default comparator for the vector.
   * @param vector the vector.
   * @param <T> the vector type.
   * @return the default comparator.
   */
  public static <T extends ValueVector> VectorValueComparator<T> createDefaultComparator(T vector) {
    if (vector instanceof BaseFixedWidthVector) {
      if (vector instanceof TinyIntVector) {
        return (VectorValueComparator<T>) new ByteComparator();
      } else if (vector instanceof SmallIntVector) {
        return (VectorValueComparator<T>) new ShortComparator();
      } else if (vector instanceof IntVector) {
        return (VectorValueComparator<T>) new IntComparator();
      } else if (vector instanceof BigIntVector) {
        return (VectorValueComparator<T>) new LongComparator();
      } else if (vector instanceof Float4Vector) {
        return (VectorValueComparator<T>) new Float4Comparator();
      } else if (vector instanceof Float8Vector) {
        return (VectorValueComparator<T>) new Float8Comparator();
      } else if (vector instanceof UInt1Vector) {
        return (VectorValueComparator<T>) new UInt1Comparator();
      } else if (vector instanceof UInt2Vector) {
        return (VectorValueComparator<T>) new UInt2Comparator();
      } else if (vector instanceof UInt4Vector) {
        return (VectorValueComparator<T>) new UInt4Comparator();
      } else if (vector instanceof UInt8Vector) {
        return (VectorValueComparator<T>) new UInt8Comparator();
      } else if (vector instanceof BitVector) {
        return (VectorValueComparator<T>) new BitComparator();
      } else if (vector instanceof DateDayVector) {
        return (VectorValueComparator<T>) new DateDayComparator();
      } else if (vector instanceof DateMilliVector) {
        return (VectorValueComparator<T>) new DateMilliComparator();
      } else if (vector instanceof Decimal256Vector) {
        return (VectorValueComparator<T>) new Decimal256Comparator();
      } else if (vector instanceof DecimalVector) {
        return (VectorValueComparator<T>) new DecimalComparator();
      } else if (vector instanceof DurationVector) {
        return (VectorValueComparator<T>) new DurationComparator();
      } else if (vector instanceof IntervalDayVector) {
        return (VectorValueComparator<T>) new IntervalDayComparator();
      } else if (vector instanceof IntervalMonthDayNanoVector) {
        throw new IllegalArgumentException("No default comparator for " + vector.getClass().getCanonicalName());
      } else if (vector instanceof TimeMicroVector) {
        return (VectorValueComparator<T>) new TimeMicroComparator();
      } else if (vector instanceof TimeMilliVector) {
        return (VectorValueComparator<T>) new TimeMilliComparator();
      } else if (vector instanceof TimeNanoVector) {
        return (VectorValueComparator<T>) new TimeNanoComparator();
      } else if (vector instanceof TimeSecVector) {
        return (VectorValueComparator<T>) new TimeSecComparator();
      } else if (vector instanceof TimeStampVector) {
        return (VectorValueComparator<T>) new TimeStampComparator();
      } else if (vector instanceof FixedSizeBinaryVector) {
        return (VectorValueComparator<T>) new FixedSizeBinaryComparator();
      }
    } else if (vector instanceof VariableWidthVector) {
      return (VectorValueComparator<T>) new VariableWidthComparator();
    } else if (vector instanceof RepeatedValueVector) {
      VectorValueComparator<?> innerComparator =
              createDefaultComparator(((RepeatedValueVector) vector).getDataVector());
      return new RepeatedValueComparator(innerComparator);
    } else if (vector instanceof FixedSizeListVector) {
      VectorValueComparator<?> innerComparator =
          createDefaultComparator(((FixedSizeListVector) vector).getDataVector());
      return new FixedSizeListComparator(innerComparator);
    } else if (vector instanceof NullVector) {
      return (VectorValueComparator<T>) new NullComparator();
    }

    throw new IllegalArgumentException("No default comparator for " + vector.getClass().getCanonicalName());
  }

  /**
   * Default comparator for bytes.
   * The comparison is based on values, with null comes first.
   */
  public static class ByteComparator extends VectorValueComparator<TinyIntVector> {

    public ByteComparator() {
      super(Byte.SIZE / 8);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      byte value1 = vector1.get(index1);
      byte value2 = vector2.get(index2);
      return value1 - value2;
    }

    @Override
    public VectorValueComparator<TinyIntVector> createNew() {
      return new ByteComparator();
    }
  }

  /**
   * Default comparator for short integers.
   * The comparison is based on values, with null comes first.
   */
  public static class ShortComparator extends VectorValueComparator<SmallIntVector> {

    public ShortComparator() {
      super(Short.SIZE / 8);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      short value1 = vector1.get(index1);
      short value2 = vector2.get(index2);
      return value1 - value2;
    }

    @Override
    public VectorValueComparator<SmallIntVector> createNew() {
      return new ShortComparator();
    }
  }

  /**
   * Default comparator for 32-bit integers.
   * The comparison is based on int values, with null comes first.
   */
  public static class IntComparator extends VectorValueComparator<IntVector> {

    public IntComparator() {
      super(Integer.SIZE / 8);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      int value1 = vector1.get(index1);
      int value2 = vector2.get(index2);
      return Integer.compare(value1, value2);
    }

    @Override
    public VectorValueComparator<IntVector> createNew() {
      return new IntComparator();
    }
  }

  /**
   * Default comparator for long integers.
   * The comparison is based on values, with null comes first.
   */
  public static class LongComparator extends VectorValueComparator<BigIntVector> {

    public LongComparator() {
      super(Long.SIZE / 8);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      long value1 = vector1.get(index1);
      long value2 = vector2.get(index2);

      return Long.compare(value1, value2);
    }

    @Override
    public VectorValueComparator<BigIntVector> createNew() {
      return new LongComparator();
    }
  }

  /**
   * Default comparator for unsigned bytes.
   * The comparison is based on values, with null comes first.
   */
  public static class UInt1Comparator extends VectorValueComparator<UInt1Vector> {

    public UInt1Comparator() {
      super(1);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      byte value1 = vector1.get(index1);
      byte value2 = vector2.get(index2);

      return (value1 & 0xff) - (value2 & 0xff);
    }

    @Override
    public VectorValueComparator<UInt1Vector> createNew() {
      return new UInt1Comparator();
    }
  }

  /**
   * Default comparator for unsigned short integer.
   * The comparison is based on values, with null comes first.
   */
  public static class UInt2Comparator extends VectorValueComparator<UInt2Vector> {

    public UInt2Comparator() {
      super(2);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      char value1 = vector1.get(index1);
      char value2 = vector2.get(index2);

      // please note that we should not use the built-in
      // Character#compare method here, as that method
      // essentially compares char values as signed integers.
      return (value1 & 0xffff) - (value2 & 0xffff);
    }

    @Override
    public VectorValueComparator<UInt2Vector> createNew() {
      return new UInt2Comparator();
    }
  }

  /**
   * Default comparator for unsigned integer.
   * The comparison is based on values, with null comes first.
   */
  public static class UInt4Comparator extends VectorValueComparator<UInt4Vector> {

    public UInt4Comparator() {
      super(4);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      int value1 = vector1.get(index1);
      int value2 = vector2.get(index2);
      return ByteFunctionHelpers.unsignedIntCompare(value1, value2);
    }

    @Override
    public VectorValueComparator<UInt4Vector> createNew() {
      return new UInt4Comparator();
    }
  }

  /**
   * Default comparator for unsigned long integer.
   * The comparison is based on values, with null comes first.
   */
  public static class UInt8Comparator extends VectorValueComparator<UInt8Vector> {

    public UInt8Comparator() {
      super(8);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      long value1 = vector1.get(index1);
      long value2 = vector2.get(index2);
      return ByteFunctionHelpers.unsignedLongCompare(value1, value2);
    }

    @Override
    public VectorValueComparator<UInt8Vector> createNew() {
      return new UInt8Comparator();
    }
  }

  /**
   * Default comparator for float type.
   * The comparison is based on values, with null comes first.
   */
  public static class Float4Comparator extends VectorValueComparator<Float4Vector> {

    public Float4Comparator() {
      super(Float.SIZE / 8);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      float value1 = vector1.get(index1);
      float value2 = vector2.get(index2);

      boolean isNan1 = Float.isNaN(value1);
      boolean isNan2 = Float.isNaN(value2);
      if (isNan1 || isNan2) {
        if (isNan1 && isNan2) {
          return 0;
        } else if (isNan1) {
          // nan is greater than any normal value
          return 1;
        } else {
          return -1;
        }
      }

      return (int) Math.signum(value1 - value2);
    }

    @Override
    public VectorValueComparator<Float4Vector> createNew() {
      return new Float4Comparator();
    }
  }

  /**
   * Default comparator for double type.
   * The comparison is based on values, with null comes first.
   */
  public static class Float8Comparator extends VectorValueComparator<Float8Vector> {

    public Float8Comparator() {
      super(Double.SIZE / 8);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      double value1 = vector1.get(index1);
      double value2 = vector2.get(index2);

      boolean isNan1 = Double.isNaN(value1);
      boolean isNan2 = Double.isNaN(value2);
      if (isNan1 || isNan2) {
        if (isNan1 && isNan2) {
          return 0;
        } else if (isNan1) {
          // nan is greater than any normal value
          return 1;
        } else {
          return -1;
        }
      }

      return (int) Math.signum(value1 - value2);
    }

    @Override
    public VectorValueComparator<Float8Vector> createNew() {
      return new Float8Comparator();
    }
  }

  /**
   * Default comparator for bit type.
   * The comparison is based on values, with null comes first.
   */
  public static class BitComparator extends VectorValueComparator<BitVector> {

    public BitComparator() {
      super(-1);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      boolean value1 = vector1.get(index1) != 0;
      boolean value2 = vector2.get(index2) != 0;

      return Boolean.compare(value1, value2);
    }

    @Override
    public VectorValueComparator<BitVector> createNew() {
      return new BitComparator();
    }
  }

  /**
   * Default comparator for DateDay type.
   * The comparison is based on values, with null comes first.
   */
  public static class DateDayComparator extends VectorValueComparator<DateDayVector> {

    public DateDayComparator() {
      super(DateDayVector.TYPE_WIDTH);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      int value1 = vector1.get(index1);
      int value2 = vector2.get(index2);
      return Integer.compare(value1, value2);
    }

    @Override
    public VectorValueComparator<DateDayVector> createNew() {
      return new DateDayComparator();
    }
  }

  /**
   * Default comparator for DateMilli type.
   * The comparison is based on values, with null comes first.
   */
  public static class DateMilliComparator extends VectorValueComparator<DateMilliVector> {

    public DateMilliComparator() {
      super(DateMilliVector.TYPE_WIDTH);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      long value1 = vector1.get(index1);
      long value2 = vector2.get(index2);

      return Long.compare(value1, value2);
    }

    @Override
    public VectorValueComparator<DateMilliVector> createNew() {
      return new DateMilliComparator();
    }
  }

  /**
   * Default comparator for Decimal256 type.
   * The comparison is based on values, with null comes first.
   */
  public static class Decimal256Comparator extends VectorValueComparator<Decimal256Vector> {

    public Decimal256Comparator() {
      super(Decimal256Vector.TYPE_WIDTH);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      BigDecimal value1 = vector1.getObjectNotNull(index1);
      BigDecimal value2 = vector2.getObjectNotNull(index2);

      return value1.compareTo(value2);
    }

    @Override
    public VectorValueComparator<Decimal256Vector> createNew() {
      return new Decimal256Comparator();
    }
  }

  /**
   * Default comparator for Decimal type.
   * The comparison is based on values, with null comes first.
   */
  public static class DecimalComparator extends VectorValueComparator<DecimalVector> {

    public DecimalComparator() {
      super(DecimalVector.TYPE_WIDTH);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      BigDecimal value1 = vector1.getObjectNotNull(index1);
      BigDecimal value2 = vector2.getObjectNotNull(index2);

      return value1.compareTo(value2);
    }

    @Override
    public VectorValueComparator<DecimalVector> createNew() {
      return new DecimalComparator();
    }
  }

  /**
   * Default comparator for Duration type.
   * The comparison is based on values, with null comes first.
   */
  public static class DurationComparator extends VectorValueComparator<DurationVector> {

    public DurationComparator() {
      super(DurationVector.TYPE_WIDTH);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      Duration value1 = vector1.getObjectNotNull(index1);
      Duration value2 = vector2.getObjectNotNull(index2);

      return value1.compareTo(value2);
    }

    @Override
    public VectorValueComparator<DurationVector> createNew() {
      return new DurationComparator();
    }
  }

  /**
   * Default comparator for IntervalDay type.
   * The comparison is based on values, with null comes first.
   */
  public static class IntervalDayComparator extends VectorValueComparator<IntervalDayVector> {

    public IntervalDayComparator() {
      super(IntervalDayVector.TYPE_WIDTH);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      Duration value1 = vector1.getObjectNotNull(index1);
      Duration value2 = vector2.getObjectNotNull(index2);

      return value1.compareTo(value2);
    }

    @Override
    public VectorValueComparator<IntervalDayVector> createNew() {
      return new IntervalDayComparator();
    }
  }

  /**
   * Default comparator for TimeMicro type.
   * The comparison is based on values, with null comes first.
   */
  public static class TimeMicroComparator extends VectorValueComparator<TimeMicroVector> {

    public TimeMicroComparator() {
      super(TimeMicroVector.TYPE_WIDTH);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      long value1 = vector1.get(index1);
      long value2 = vector2.get(index2);

      return Long.compare(value1, value2);
    }

    @Override
    public VectorValueComparator<TimeMicroVector> createNew() {
      return new TimeMicroComparator();
    }
  }

  /**
   * Default comparator for TimeMilli type.
   * The comparison is based on values, with null comes first.
   */
  public static class TimeMilliComparator extends VectorValueComparator<TimeMilliVector> {

    public TimeMilliComparator() {
      super(TimeMilliVector.TYPE_WIDTH);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      int value1 = vector1.get(index1);
      int value2 = vector2.get(index2);

      return Integer.compare(value1, value2);
    }

    @Override
    public VectorValueComparator<TimeMilliVector> createNew() {
      return new TimeMilliComparator();
    }
  }

  /**
   * Default comparator for TimeNano type.
   * The comparison is based on values, with null comes first.
   */
  public static class TimeNanoComparator extends VectorValueComparator<TimeNanoVector> {

    public TimeNanoComparator() {
      super(TimeNanoVector.TYPE_WIDTH);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      long value1 = vector1.get(index1);
      long value2 = vector2.get(index2);

      return Long.compare(value1, value2);
    }

    @Override
    public VectorValueComparator<TimeNanoVector> createNew() {
      return new TimeNanoComparator();
    }
  }

  /**
   * Default comparator for TimeSec type.
   * The comparison is based on values, with null comes first.
   */
  public static class TimeSecComparator extends VectorValueComparator<TimeSecVector> {

    public TimeSecComparator() {
      super(TimeSecVector.TYPE_WIDTH);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      int value1 = vector1.get(index1);
      int value2 = vector2.get(index2);

      return Integer.compare(value1, value2);
    }

    @Override
    public VectorValueComparator<TimeSecVector> createNew() {
      return new TimeSecComparator();
    }
  }

  /**
   * Default comparator for TimeSec type.
   * The comparison is based on values, with null comes first.
   */
  public static class TimeStampComparator extends VectorValueComparator<TimeStampVector> {

    public TimeStampComparator() {
      super(TimeStampVector.TYPE_WIDTH);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      long value1 = vector1.get(index1);
      long value2 = vector2.get(index2);

      return Long.compare(value1, value2);
    }

    @Override
    public VectorValueComparator<TimeStampVector> createNew() {
      return new TimeStampComparator();
    }
  }

  /**
   * Default comparator for {@link org.apache.arrow.vector.FixedSizeBinaryVector}.
   * The comparison is in lexicographic order, with null comes first.
   */
  public static class FixedSizeBinaryComparator extends VectorValueComparator<FixedSizeBinaryVector> {

    @Override
    public int compare(int index1, int index2) {
      NullableFixedSizeBinaryHolder holder1 = new NullableFixedSizeBinaryHolder();
      NullableFixedSizeBinaryHolder holder2 = new NullableFixedSizeBinaryHolder();
      vector1.get(index1, holder1);
      vector2.get(index2, holder2);

      return ByteFunctionHelpers.compare(
          holder1.buffer, 0, holder1.byteWidth, holder2.buffer, 0, holder2.byteWidth);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      NullableFixedSizeBinaryHolder holder1 = new NullableFixedSizeBinaryHolder();
      NullableFixedSizeBinaryHolder holder2 = new NullableFixedSizeBinaryHolder();
      vector1.get(index1, holder1);
      vector2.get(index2, holder2);

      return ByteFunctionHelpers.compare(
          holder1.buffer, 0, holder1.byteWidth, holder2.buffer, 0, holder2.byteWidth);
    }

    @Override
    public VectorValueComparator<FixedSizeBinaryVector> createNew() {
      return new FixedSizeBinaryComparator();
    }
  }

  /**
   * Default comparator for {@link org.apache.arrow.vector.NullVector}.
   */
  public static class NullComparator extends VectorValueComparator<NullVector> {
    @Override
    public int compare(int index1, int index2) {
      // Values are always equal (and are always null).
      return 0;
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      throw new AssertionError("Cannot compare non-null values in a NullVector.");
    }

    @Override
    public VectorValueComparator<NullVector> createNew() {
      return new NullComparator();
    }
  }

  /**
   * Default comparator for {@link org.apache.arrow.vector.VariableWidthVector}.
   * The comparison is in lexicographic order, with null comes first.
   */
  public static class VariableWidthComparator extends VectorValueComparator<VariableWidthVector> {

    private final ArrowBufPointer reusablePointer1 = new ArrowBufPointer();

    private final ArrowBufPointer reusablePointer2 = new ArrowBufPointer();

    @Override
    public int compare(int index1, int index2) {
      vector1.getDataPointer(index1, reusablePointer1);
      vector2.getDataPointer(index2, reusablePointer2);
      return reusablePointer1.compareTo(reusablePointer2);
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      vector1.getDataPointer(index1, reusablePointer1);
      vector2.getDataPointer(index2, reusablePointer2);
      return reusablePointer1.compareTo(reusablePointer2);
    }

    @Override
    public VectorValueComparator<VariableWidthVector> createNew() {
      return new VariableWidthComparator();
    }
  }

  /**
   * Default comparator for {@link RepeatedValueVector}.
   * It works by comparing the underlying vector in a lexicographic order.
   * @param <T> inner vector type.
   */
  public static class RepeatedValueComparator<T extends ValueVector>
          extends VectorValueComparator<RepeatedValueVector> {

    private final VectorValueComparator<T> innerComparator;

    public RepeatedValueComparator(VectorValueComparator<T> innerComparator) {
      this.innerComparator = innerComparator;
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      int startIdx1 = vector1.getOffsetBuffer().getInt((long) index1 * OFFSET_WIDTH);
      int startIdx2 = vector2.getOffsetBuffer().getInt((long) index2 * OFFSET_WIDTH);

      int endIdx1 = vector1.getOffsetBuffer().getInt((long) (index1 + 1) * OFFSET_WIDTH);
      int endIdx2 = vector2.getOffsetBuffer().getInt((long) (index2 + 1) * OFFSET_WIDTH);

      int length1 = endIdx1 - startIdx1;
      int length2 = endIdx2 - startIdx2;

      int length = Math.min(length1, length2);

      for (int i = 0; i < length; i++) {
        int result = innerComparator.compare(startIdx1 + i, startIdx2 + i);
        if (result != 0) {
          return result;
        }
      }
      return length1 - length2;
    }

    @Override
    public VectorValueComparator<RepeatedValueVector> createNew() {
      VectorValueComparator<T> newInnerComparator = innerComparator.createNew();
      return new RepeatedValueComparator<>(newInnerComparator);
    }

    @Override
    public void attachVectors(RepeatedValueVector vector1, RepeatedValueVector vector2) {
      this.vector1 = vector1;
      this.vector2 = vector2;

      innerComparator.attachVectors((T) vector1.getDataVector(), (T) vector2.getDataVector());
    }
  }

  /**
   * Default comparator for {@link RepeatedValueVector}.
   * It works by comparing the underlying vector in a lexicographic order.
   * @param <T> inner vector type.
   */
  public static class FixedSizeListComparator<T extends ValueVector>
      extends VectorValueComparator<FixedSizeListVector> {

    private final VectorValueComparator<T> innerComparator;

    public FixedSizeListComparator(VectorValueComparator<T> innerComparator) {
      this.innerComparator = innerComparator;
    }

    @Override
    public int compareNotNull(int index1, int index2) {
      int length1 = vector1.getListSize();
      int length2 = vector2.getListSize();

      int length = Math.min(length1, length2);
      int startIdx1 = vector1.getElementStartIndex(index1);
      int startIdx2 = vector2.getElementStartIndex(index2);

      for (int i = 0; i < length; i++) {
        int result = innerComparator.compare(startIdx1 + i, startIdx2 + i);
        if (result != 0) {
          return result;
        }
      }
      return length1 - length2;
    }

    @Override
    public VectorValueComparator<FixedSizeListVector> createNew() {
      VectorValueComparator<T> newInnerComparator = innerComparator.createNew();
      return new FixedSizeListComparator<>(newInnerComparator);
    }

    @Override
    public void attachVectors(FixedSizeListVector vector1, FixedSizeListVector vector2) {
      this.vector1 = vector1;
      this.vector2 = vector2;

      innerComparator.attachVectors((T) vector1.getDataVector(), (T) vector2.getDataVector());
    }
  }

  private DefaultVectorComparators() {
  }
}

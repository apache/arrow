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

package org.apache.arrow.driver.jdbc.utils;

import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListWriter;
import org.apache.arrow.vector.complex.impl.UnionLargeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class RootAllocatorTestRule implements TestRule, AutoCloseable {

  public static final byte MAX_VALUE = Byte.MAX_VALUE;
  private final BufferAllocator rootAllocator = new RootAllocator();

  private final Random random = new Random(10);

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          base.evaluate();
        } finally {
          close();
        }
      }
    };
  }

  public BufferAllocator getRootAllocator() {
    return rootAllocator;
  }

  @Override
  public void close() throws Exception {
    this.rootAllocator.getChildAllocators().forEach(BufferAllocator::close);
    AutoCloseables.close(this.rootAllocator);
  }

  /**
   * Create a Float8Vector to be used in the accessor tests.
   *
   * @return Float8Vector
   */
  public Float8Vector createFloat8Vector() {
    double[] doubleVectorValues = new double[] {
        0,
        1,
        -1,
        Byte.MIN_VALUE,
        Byte.MAX_VALUE,
        Short.MIN_VALUE,
        Short.MAX_VALUE,
        Integer.MIN_VALUE,
        Integer.MAX_VALUE,
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        Float.MAX_VALUE,
        -Float.MAX_VALUE,
        Float.NEGATIVE_INFINITY,
        Float.POSITIVE_INFINITY,
        Float.MIN_VALUE,
        -Float.MIN_VALUE,
        Double.MAX_VALUE,
        -Double.MAX_VALUE,
        Double.NEGATIVE_INFINITY,
        Double.POSITIVE_INFINITY,
        Double.MIN_VALUE,
        -Double.MIN_VALUE,
    };

    Float8Vector result = new Float8Vector("", this.getRootAllocator());
    result.setValueCount(MAX_VALUE);
    for (int i = 0; i < MAX_VALUE; i++) {
      if (i < doubleVectorValues.length) {
        result.setSafe(i, doubleVectorValues[i]);
      } else {
        result.setSafe(i, random.nextDouble());
      }
    }

    return result;
  }

  public Float8Vector createFloat8VectorForNullTests() {
    final Float8Vector float8Vector = new Float8Vector("ID", this.getRootAllocator());
    float8Vector.allocateNew(1);
    float8Vector.setNull(0);
    float8Vector.setValueCount(1);

    return float8Vector;
  }

  /**
   * Create a Float4Vector to be used in the accessor tests.
   *
   * @return Float4Vector
   */
  public Float4Vector createFloat4Vector() {

    float[] floatVectorValues = new float[] {
        0,
        1,
        -1,
        Byte.MIN_VALUE,
        Byte.MAX_VALUE,
        Short.MIN_VALUE,
        Short.MAX_VALUE,
        Integer.MIN_VALUE,
        Integer.MAX_VALUE,
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        Float.MAX_VALUE,
        -Float.MAX_VALUE,
        Float.NEGATIVE_INFINITY,
        Float.POSITIVE_INFINITY,
        Float.MIN_VALUE,
        -Float.MIN_VALUE,
    };

    Float4Vector result = new Float4Vector("", this.getRootAllocator());
    result.setValueCount(MAX_VALUE);
    for (int i = 0; i < MAX_VALUE; i++) {
      if (i < floatVectorValues.length) {
        result.setSafe(i, floatVectorValues[i]);
      } else {
        result.setSafe(i, random.nextFloat());
      }
    }

    return result;
  }

  /**
   * Create a IntVector to be used in the accessor tests.
   *
   * @return IntVector
   */
  public IntVector createIntVector() {

    int[] intVectorValues = new int[] {
        0,
        1,
        -1,
        Byte.MIN_VALUE,
        Byte.MAX_VALUE,
        Short.MIN_VALUE,
        Short.MAX_VALUE,
        Integer.MIN_VALUE,
        Integer.MAX_VALUE,
    };

    IntVector result = new IntVector("", this.getRootAllocator());
    result.setValueCount(MAX_VALUE);
    for (int i = 0; i < MAX_VALUE; i++) {
      if (i < intVectorValues.length) {
        result.setSafe(i, intVectorValues[i]);
      } else {
        result.setSafe(i, random.nextInt());
      }
    }

    return result;
  }

  /**
   * Create a SmallIntVector to be used in the accessor tests.
   *
   * @return SmallIntVector
   */
  public SmallIntVector createSmallIntVector() {

    short[] smallIntVectorValues = new short[] {
        0,
        1,
        -1,
        Byte.MIN_VALUE,
        Byte.MAX_VALUE,
        Short.MIN_VALUE,
        Short.MAX_VALUE,
    };

    SmallIntVector result = new SmallIntVector("", this.getRootAllocator());
    result.setValueCount(MAX_VALUE);
    for (int i = 0; i < MAX_VALUE; i++) {
      if (i < smallIntVectorValues.length) {
        result.setSafe(i, smallIntVectorValues[i]);
      } else {
        result.setSafe(i, random.nextInt(Short.MAX_VALUE));
      }
    }

    return result;
  }

  /**
   * Create a TinyIntVector to be used in the accessor tests.
   *
   * @return TinyIntVector
   */
  public TinyIntVector createTinyIntVector() {

    byte[] byteVectorValues = new byte[] {
        0,
        1,
        -1,
        Byte.MIN_VALUE,
        Byte.MAX_VALUE,
    };

    TinyIntVector result = new TinyIntVector("", this.getRootAllocator());
    result.setValueCount(MAX_VALUE);
    for (int i = 0; i < MAX_VALUE; i++) {
      if (i < byteVectorValues.length) {
        result.setSafe(i, byteVectorValues[i]);
      } else {
        result.setSafe(i, random.nextInt(Byte.MAX_VALUE));
      }
    }

    return result;
  }

  /**
   * Create a BigIntVector to be used in the accessor tests.
   *
   * @return BigIntVector
   */
  public BigIntVector createBigIntVector() {

    long[] longVectorValues = new long[] {
        0,
        1,
        -1,
        Byte.MIN_VALUE,
        Byte.MAX_VALUE,
        Short.MIN_VALUE,
        Short.MAX_VALUE,
        Integer.MIN_VALUE,
        Integer.MAX_VALUE,
        Long.MIN_VALUE,
        Long.MAX_VALUE,
    };

    BigIntVector result = new BigIntVector("", this.getRootAllocator());
    result.setValueCount(MAX_VALUE);
    for (int i = 0; i < MAX_VALUE; i++) {
      if (i < longVectorValues.length) {
        result.setSafe(i, longVectorValues[i]);
      } else {
        result.setSafe(i, random.nextLong());
      }
    }

    return result;
  }

  /**
   * Create a UInt1Vector to be used in the accessor tests.
   *
   * @return UInt1Vector
   */
  public UInt1Vector createUInt1Vector() {

    short[] uInt1VectorValues = new short[] {
        0,
        1,
        -1,
        Byte.MIN_VALUE,
        Byte.MAX_VALUE,
    };

    UInt1Vector result = new UInt1Vector("", this.getRootAllocator());
    result.setValueCount(MAX_VALUE);
    for (int i = 0; i < MAX_VALUE; i++) {
      if (i < uInt1VectorValues.length) {
        result.setSafe(i, uInt1VectorValues[i]);
      } else {
        result.setSafe(i, random.nextInt(0x100));
      }
    }

    return result;
  }

  /**
   * Create a UInt2Vector to be used in the accessor tests.
   *
   * @return UInt2Vector
   */
  public UInt2Vector createUInt2Vector() {

    int[] uInt2VectorValues = new int[] {
        0,
        1,
        -1,
        Byte.MIN_VALUE,
        Byte.MAX_VALUE,
        Short.MIN_VALUE,
        Short.MAX_VALUE,
    };

    UInt2Vector result = new UInt2Vector("", this.getRootAllocator());
    result.setValueCount(MAX_VALUE);
    for (int i = 0; i < MAX_VALUE; i++) {
      if (i < uInt2VectorValues.length) {
        result.setSafe(i, uInt2VectorValues[i]);
      } else {
        result.setSafe(i, random.nextInt(0x10000));
      }
    }

    return result;
  }

  /**
   * Create a UInt4Vector to be used in the accessor tests.
   *
   * @return UInt4Vector
   */
  public UInt4Vector createUInt4Vector() {


    int[] uInt4VectorValues = new int[] {
        0,
        1,
        -1,
        Byte.MIN_VALUE,
        Byte.MAX_VALUE,
        Short.MIN_VALUE,
        Short.MAX_VALUE,
        Integer.MIN_VALUE,
        Integer.MAX_VALUE
    };

    UInt4Vector result = new UInt4Vector("", this.getRootAllocator());
    result.setValueCount(MAX_VALUE);
    for (int i = 0; i < MAX_VALUE; i++) {
      if (i < uInt4VectorValues.length) {
        result.setSafe(i, uInt4VectorValues[i]);
      } else {
        result.setSafe(i, random.nextInt(Integer.MAX_VALUE));
      }
    }

    return result;
  }

  /**
   * Create a UInt8Vector to be used in the accessor tests.
   *
   * @return UInt8Vector
   */
  public UInt8Vector createUInt8Vector() {

    long[] uInt8VectorValues = new long[] {
        0,
        1,
        -1,
        Byte.MIN_VALUE,
        Byte.MAX_VALUE,
        Short.MIN_VALUE,
        Short.MAX_VALUE,
        Integer.MIN_VALUE,
        Integer.MAX_VALUE,
        Long.MIN_VALUE,
        Long.MAX_VALUE
    };

    UInt8Vector result = new UInt8Vector("", this.getRootAllocator());
    result.setValueCount(MAX_VALUE);
    for (int i = 0; i < MAX_VALUE; i++) {
      if (i < uInt8VectorValues.length) {
        result.setSafe(i, uInt8VectorValues[i]);
      } else {
        result.setSafe(i, random.nextLong());
      }
    }

    return result;
  }

  /**
   * Create a VarBinaryVector to be used in the accessor tests.
   *
   * @return VarBinaryVector
   */
  public VarBinaryVector createVarBinaryVector() {
    return createVarBinaryVector("");
  }

  /**
   * Create a VarBinaryVector to be used in the accessor tests.
   *
   * @return VarBinaryVector
   */
  public VarBinaryVector createVarBinaryVector(final String fieldName) {
    VarBinaryVector valueVector = new VarBinaryVector(fieldName, this.getRootAllocator());
    valueVector.allocateNew(3);
    valueVector.setSafe(0, (fieldName + "__BINARY_DATA_0001").getBytes());
    valueVector.setSafe(1, (fieldName + "__BINARY_DATA_0002").getBytes());
    valueVector.setSafe(2, (fieldName + "__BINARY_DATA_0003").getBytes());
    valueVector.setValueCount(3);

    return valueVector;
  }

  /**
   * Create a LargeVarBinaryVector to be used in the accessor tests.
   *
   * @return LargeVarBinaryVector
   */
  public LargeVarBinaryVector createLargeVarBinaryVector() {
    LargeVarBinaryVector valueVector = new LargeVarBinaryVector("", this.getRootAllocator());
    valueVector.allocateNew(3);
    valueVector.setSafe(0, "BINARY_DATA_0001".getBytes());
    valueVector.setSafe(1, "BINARY_DATA_0002".getBytes());
    valueVector.setSafe(2, "BINARY_DATA_0003".getBytes());
    valueVector.setValueCount(3);

    return valueVector;
  }

  /**
   * Create a FixedSizeBinaryVector to be used in the accessor tests.
   *
   * @return FixedSizeBinaryVector
   */
  public FixedSizeBinaryVector createFixedSizeBinaryVector() {
    FixedSizeBinaryVector valueVector = new FixedSizeBinaryVector("", this.getRootAllocator(), 16);
    valueVector.allocateNew(3);
    valueVector.setSafe(0, "BINARY_DATA_0001".getBytes());
    valueVector.setSafe(1, "BINARY_DATA_0002".getBytes());
    valueVector.setSafe(2, "BINARY_DATA_0003".getBytes());
    valueVector.setValueCount(3);

    return valueVector;
  }

  /**
   * Create a UInt8Vector to be used in the accessor tests.
   *
   * @return UInt8Vector
   */
  public DecimalVector createDecimalVector() {

    BigDecimal[] bigDecimalValues = new BigDecimal[] {
        new BigDecimal(0),
        new BigDecimal(1),
        new BigDecimal(-1),
        new BigDecimal(Byte.MIN_VALUE),
        new BigDecimal(Byte.MAX_VALUE),
        new BigDecimal(-Short.MAX_VALUE),
        new BigDecimal(Short.MIN_VALUE),
        new BigDecimal(Integer.MIN_VALUE),
        new BigDecimal(Integer.MAX_VALUE),
        new BigDecimal(Long.MIN_VALUE),
        new BigDecimal(-Long.MAX_VALUE),
        new BigDecimal("170141183460469231731687303715884105727")
    };

    DecimalVector result = new DecimalVector("ID", this.getRootAllocator(), 39, 0);
    result.setValueCount(MAX_VALUE);
    for (int i = 0; i < MAX_VALUE; i++) {
      if (i < bigDecimalValues.length) {
        result.setSafe(i, bigDecimalValues[i]);
      } else {
        result.setSafe(i, random.nextLong());
      }
    }

    return result;
  }

  /**
   * Create a UInt8Vector to be used in the accessor tests.
   *
   * @return UInt8Vector
   */
  public Decimal256Vector createDecimal256Vector() {

    BigDecimal[] bigDecimalValues = new BigDecimal[] {
        new BigDecimal(0),
        new BigDecimal(1),
        new BigDecimal(-1),
        new BigDecimal(Byte.MIN_VALUE),
        new BigDecimal(Byte.MAX_VALUE),
        new BigDecimal(-Short.MAX_VALUE),
        new BigDecimal(Short.MIN_VALUE),
        new BigDecimal(Integer.MIN_VALUE),
        new BigDecimal(Integer.MAX_VALUE),
        new BigDecimal(Long.MIN_VALUE),
        new BigDecimal(-Long.MAX_VALUE),
        new BigDecimal("170141183460469231731687303715884105727"),
        new BigDecimal("17014118346046923173168234157303715884105727"),
        new BigDecimal("1701411834604692317316823415265417303715884105727"),
        new BigDecimal("-17014118346046923173168234152654115451237303715884105727"),
        new BigDecimal("-17014118346046923173168234152654115451231545157303715884105727"),
        new BigDecimal("1701411834604692315815656534152654115451231545157303715884105727"),
        new BigDecimal("30560141183460469231581565634152654115451231545157303715884105727"),
        new BigDecimal(
            "57896044618658097711785492504343953926634992332820282019728792003956564819967"),
        new BigDecimal(
            "-56896044618658097711785492504343953926634992332820282019728792003956564819967")
    };

    Decimal256Vector result = new Decimal256Vector("ID", this.getRootAllocator(), 77, 0);
    result.setValueCount(MAX_VALUE);
    for (int i = 0; i < MAX_VALUE; i++) {
      if (i < bigDecimalValues.length) {
        result.setSafe(i, bigDecimalValues[i]);
      } else {
        result.setSafe(i, random.nextLong());
      }
    }

    return result;
  }

  public TimeStampNanoVector createTimeStampNanoVector() {
    TimeStampNanoVector valueVector = new TimeStampNanoVector("", this.getRootAllocator());
    valueVector.allocateNew(2);
    valueVector.setSafe(0, TimeUnit.MILLISECONDS.toNanos(1625702400000L));
    valueVector.setSafe(1, TimeUnit.MILLISECONDS.toNanos(1625788800000L));
    valueVector.setValueCount(2);

    return valueVector;
  }

  public TimeStampNanoTZVector createTimeStampNanoTZVector(String timeZone) {
    TimeStampNanoTZVector valueVector =
        new TimeStampNanoTZVector("", this.getRootAllocator(), timeZone);
    valueVector.allocateNew(2);
    valueVector.setSafe(0, TimeUnit.MILLISECONDS.toNanos(1625702400000L));
    valueVector.setSafe(1, TimeUnit.MILLISECONDS.toNanos(1625788800000L));
    valueVector.setValueCount(2);

    return valueVector;
  }

  public TimeStampMicroVector createTimeStampMicroVector() {
    TimeStampMicroVector valueVector = new TimeStampMicroVector("", this.getRootAllocator());
    valueVector.allocateNew(2);
    valueVector.setSafe(0, TimeUnit.MILLISECONDS.toMicros(1625702400000L));
    valueVector.setSafe(1, TimeUnit.MILLISECONDS.toMicros(1625788800000L));
    valueVector.setValueCount(2);

    return valueVector;
  }

  public TimeStampMicroTZVector createTimeStampMicroTZVector(String timeZone) {
    TimeStampMicroTZVector valueVector =
        new TimeStampMicroTZVector("", this.getRootAllocator(), timeZone);
    valueVector.allocateNew(2);
    valueVector.setSafe(0, TimeUnit.MILLISECONDS.toMicros(1625702400000L));
    valueVector.setSafe(1, TimeUnit.MILLISECONDS.toMicros(1625788800000L));
    valueVector.setValueCount(2);

    return valueVector;
  }

  public TimeStampMilliVector createTimeStampMilliVector() {
    TimeStampMilliVector valueVector = new TimeStampMilliVector("", this.getRootAllocator());
    valueVector.allocateNew(2);
    valueVector.setSafe(0, 1625702400000L);
    valueVector.setSafe(1, 1625788800000L);
    valueVector.setValueCount(2);

    return valueVector;
  }

  public TimeStampMilliTZVector createTimeStampMilliTZVector(String timeZone) {
    TimeStampMilliTZVector valueVector =
        new TimeStampMilliTZVector("", this.getRootAllocator(), timeZone);
    valueVector.allocateNew(2);
    valueVector.setSafe(0, 1625702400000L);
    valueVector.setSafe(1, 1625788800000L);
    valueVector.setValueCount(2);

    return valueVector;
  }

  public TimeStampSecVector createTimeStampSecVector() {
    TimeStampSecVector valueVector = new TimeStampSecVector("", this.getRootAllocator());
    valueVector.allocateNew(2);
    valueVector.setSafe(0, TimeUnit.MILLISECONDS.toSeconds(1625702400000L));
    valueVector.setSafe(1, TimeUnit.MILLISECONDS.toSeconds(1625788800000L));
    valueVector.setValueCount(2);

    return valueVector;
  }

  public TimeStampSecTZVector createTimeStampSecTZVector(String timeZone) {
    TimeStampSecTZVector valueVector =
        new TimeStampSecTZVector("", this.getRootAllocator(), timeZone);
    valueVector.allocateNew(2);
    valueVector.setSafe(0, TimeUnit.MILLISECONDS.toSeconds(1625702400000L));
    valueVector.setSafe(1, TimeUnit.MILLISECONDS.toSeconds(1625788800000L));
    valueVector.setValueCount(2);

    return valueVector;
  }

  public BitVector createBitVector() {
    BitVector valueVector = new BitVector("Value", this.getRootAllocator());
    valueVector.allocateNew(2);
    valueVector.setSafe(0, 0);
    valueVector.setSafe(1, 1);
    valueVector.setValueCount(2);

    return valueVector;
  }

  public BitVector createBitVectorForNullTests() {
    final BitVector bitVector = new BitVector("ID", this.getRootAllocator());
    bitVector.allocateNew(2);
    bitVector.setNull(0);
    bitVector.setValueCount(1);

    return bitVector;
  }

  public TimeNanoVector createTimeNanoVector() {
    TimeNanoVector valueVector = new TimeNanoVector("", this.getRootAllocator());
    valueVector.allocateNew(5);
    valueVector.setSafe(0, 0);
    valueVector.setSafe(1, 1_000_000_000L); // 1 second
    valueVector.setSafe(2, 60 * 1_000_000_000L); // 1 minute
    valueVector.setSafe(3, 60 * 60 * 1_000_000_000L); // 1 hour
    valueVector.setSafe(4, (24 * 60 * 60 - 1) * 1_000_000_000L); // 23:59:59
    valueVector.setValueCount(5);

    return valueVector;
  }

  public TimeMicroVector createTimeMicroVector() {
    TimeMicroVector valueVector = new TimeMicroVector("", this.getRootAllocator());
    valueVector.allocateNew(5);
    valueVector.setSafe(0, 0);
    valueVector.setSafe(1, 1_000_000L); // 1 second
    valueVector.setSafe(2, 60 * 1_000_000L); // 1 minute
    valueVector.setSafe(3, 60 * 60 * 1_000_000L); // 1 hour
    valueVector.setSafe(4, (24 * 60 * 60 - 1) * 1_000_000L); // 23:59:59
    valueVector.setValueCount(5);

    return valueVector;
  }

  public TimeMilliVector createTimeMilliVector() {
    TimeMilliVector valueVector = new TimeMilliVector("", this.getRootAllocator());
    valueVector.allocateNew(5);
    valueVector.setSafe(0, 0);
    valueVector.setSafe(1, 1_000); // 1 second
    valueVector.setSafe(2, 60 * 1_000); // 1 minute
    valueVector.setSafe(3, 60 * 60 * 1_000); // 1 hour
    valueVector.setSafe(4, (24 * 60 * 60 - 1) * 1_000); // 23:59:59
    valueVector.setValueCount(5);

    return valueVector;
  }

  public TimeSecVector createTimeSecVector() {
    TimeSecVector valueVector = new TimeSecVector("", this.getRootAllocator());
    valueVector.allocateNew(5);
    valueVector.setSafe(0, 0);
    valueVector.setSafe(1, 1); // 1 second
    valueVector.setSafe(2, 60); // 1 minute
    valueVector.setSafe(3, 60 * 60); // 1 hour
    valueVector.setSafe(4, (24 * 60 * 60 - 1)); // 23:59:59
    valueVector.setValueCount(5);

    return valueVector;
  }

  public DateDayVector createDateDayVector() {
    DateDayVector valueVector = new DateDayVector("", this.getRootAllocator());
    valueVector.allocateNew(2);
    valueVector.setSafe(0, (int) TimeUnit.MILLISECONDS.toDays(1625702400000L));
    valueVector.setSafe(1, (int) TimeUnit.MILLISECONDS.toDays(1625788800000L));
    valueVector.setValueCount(2);

    return valueVector;
  }

  public DateMilliVector createDateMilliVector() {
    DateMilliVector valueVector = new DateMilliVector("", this.getRootAllocator());
    valueVector.allocateNew(2);
    valueVector.setSafe(0, 1625702400000L);
    valueVector.setSafe(1, 1625788800000L);
    valueVector.setValueCount(2);

    return valueVector;
  }

  public ListVector createListVector() {
    return createListVector("");
  }

  public ListVector createListVector(String fieldName) {
    ListVector valueVector = ListVector.empty(fieldName, this.getRootAllocator());
    valueVector.setInitialCapacity(MAX_VALUE);

    UnionListWriter writer = valueVector.getWriter();

    IntStream range = IntStream.range(0, MAX_VALUE);

    range.forEach(row -> {
      writer.startList();
      writer.setPosition(row);
      IntStream.range(0, 5).map(j -> j * row).forEach(writer::writeInt);
      writer.setValueCount(5);
      writer.endList();
    });

    valueVector.setValueCount(MAX_VALUE);

    return valueVector;
  }

  public LargeListVector createLargeListVector() {
    LargeListVector valueVector = LargeListVector.empty("", this.getRootAllocator());
    valueVector.setInitialCapacity(MAX_VALUE);

    UnionLargeListWriter writer = valueVector.getWriter();

    IntStream range = IntStream.range(0, MAX_VALUE);

    range.forEach(row -> {
      writer.startList();
      writer.setPosition(row);
      IntStream.range(0, 5).map(j -> j * row).forEach(writer::writeInt);
      writer.setValueCount(5);
      writer.endList();
    });

    valueVector.setValueCount(MAX_VALUE);

    return valueVector;
  }

  public FixedSizeListVector createFixedSizeListVector() {
    FixedSizeListVector valueVector = FixedSizeListVector.empty("", 5, this.getRootAllocator());
    valueVector.setInitialCapacity(MAX_VALUE);

    UnionFixedSizeListWriter writer = valueVector.getWriter();

    IntStream range = IntStream.range(0, MAX_VALUE);

    range.forEach(row -> {
      writer.startList();
      writer.setPosition(row);
      IntStream.range(0, 5).map(j -> j * row).forEach(writer::writeInt);
      writer.setValueCount(5);
      writer.endList();
    });

    valueVector.setValueCount(MAX_VALUE);

    return valueVector;
  }
}

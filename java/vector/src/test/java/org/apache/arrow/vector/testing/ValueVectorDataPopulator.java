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

package org.apache.arrow.vector.testing;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
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
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarCharVector;
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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.IntervalDayHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Utility for populating {@link org.apache.arrow.vector.ValueVector}.
 */
public class ValueVectorDataPopulator {

  private ValueVectorDataPopulator() {
  }

  /**
   * Populate values for BigIntVector.
   */
  public static void setVector(BigIntVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for BitVector.
   */
  public static void setVector(BitVector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for DateDayVector.
   * @param values numbers of days since UNIX epoch
   */
  public static void setVector(DateDayVector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for DateMilliVector.
   * @param values numbers of milliseconds since UNIX epoch
   */
  public static void setVector(DateMilliVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for DecimalVector.
   */
  public static void setVector(DecimalVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for Decimal256Vector.
   */
  public static void setVector(Decimal256Vector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for Decimal256Vector.
   */
  public static void setVector(Decimal256Vector vector, BigDecimal... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for DurationVector.
   * @param values values of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
   */
  public static void setVector(DurationVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for FixedSizeBinaryVector.
   */
  public static void setVector(FixedSizeBinaryVector vector, byte[]... values) {
    final int length = values.length;
    vector.allocateNewSafe();
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for Float4Vector.
   */
  public static void setVector(Float4Vector vector, Float... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for Float8Vector.
   */
  public static void setVector(Float8Vector vector, Double... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for IntVector.
   */
  public static void setVector(IntVector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for IntervalDayVector.
   * @param values holders witch holds days and milliseconds values which represents interval in SQL style.
   */
  public static void setVector(IntervalDayVector vector, IntervalDayHolder... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i].days, values[i].milliseconds);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for IntervalYearVector.
   * @param values total month intervals in SQL style.
   */
  public static void setVector(IntervalYearVector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for SmallIntVector.
   */
  public static void setVector(SmallIntVector vector, Short... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TimeMicroVector.
   * @param values numbers of microseconds since UNIX epoch
   */
  public static void setVector(TimeMicroVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TimeMicroVector.
   * @param values numbers of milliseconds since UNIX epoch
   */
  public static void setVector(TimeMilliVector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TimeNanoVector.
   * @param values numbers of nanoseconds since UNIX epoch
   */
  public static void setVector(TimeNanoVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TimeSecVector.
   * @param values numbers of seconds since UNIX epoch
   */
  public static void setVector(TimeSecVector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TimeStampMicroTZVector.
   * @param values numbers of microseconds since UNIX epoch
   */
  public static void setVector(TimeStampMicroTZVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TimeStampMicroVector.
   * @param values numbers of microseconds since UNIX epoch
   */
  public static void setVector(TimeStampMicroVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TimeStampMilliTZVector.
   * @param values numbers of milliseconds since UNIX epoch
   */
  public static void setVector(TimeStampMilliTZVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TimeStampMilliVector.
   * @param values numbers of milliseconds since UNIX epoch
   */
  public static void setVector(TimeStampMilliVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TimeStampNanoTZVector.
   * @param values numbers of nanoseconds since UNIX epoch
   */
  public static void setVector(TimeStampNanoTZVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TimeStampNanoVector.
   * @param values numbers of nanoseconds since UNIX epoch
   */
  public static void setVector(TimeStampNanoVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TimeStampSecTZVector.
   * @param values numbers of seconds since UNIX epoch
   */
  public static void setVector(TimeStampSecTZVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TimeStampSecVector.
   * @param values numbers of seconds since UNIX epoch
   */
  public static void setVector(TimeStampSecVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for TinyIntVector.
   */
  public static void setVector(TinyIntVector vector, Byte... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for UInt1Vector.
   */
  public static void setVector(UInt1Vector vector, Byte... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for UInt2Vector.
   */
  public static void setVector(UInt2Vector vector, Character... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for UInt4Vector.
   */
  public static void setVector(UInt4Vector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for UInt8Vector.
   */
  public static void setVector(UInt8Vector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for VarBinaryVector.
   */
  public static void setVector(VarBinaryVector vector, byte[]... values) {
    final int length = values.length;
    vector.allocateNewSafe();
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for VarCharVector.
   */
  public static void setVector(VarCharVector vector, byte[]... values) {
    final int length = values.length;
    vector.allocateNewSafe();
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for LargeVarCharVector.
   */
  public static void setVector(LargeVarCharVector vector, byte[]... values) {
    final int length = values.length;
    vector.allocateNewSafe();
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for VarCharVector.
   */
  public static void setVector(VarCharVector vector, String... values) {
    final int length = values.length;
    vector.allocateNewSafe();
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for LargeVarCharVector.
   */
  public static void setVector(LargeVarCharVector vector, String... values) {
    final int length = values.length;
    vector.allocateNewSafe();
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
      }
    }
    vector.setValueCount(length);
  }

  /**
   * Populate values for {@link ListVector}.
   */
  public static void setVector(ListVector vector, List<Integer>... values) {
    vector.allocateNewSafe();
    Types.MinorType type = Types.MinorType.INT;
    vector.addOrGetVector(FieldType.nullable(type.getType()));

    IntVector dataVector = (IntVector) vector.getDataVector();
    dataVector.allocateNew();

    // set underlying vectors
    int curPos = 0;
    vector.getOffsetBuffer().setInt(0, curPos);
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        BitVectorHelper.unsetBit(vector.getValidityBuffer(), i);
      } else {
        BitVectorHelper.setBit(vector.getValidityBuffer(), i);
        for (int value : values[i]) {
          dataVector.setSafe(curPos, value);
          curPos += 1;
        }
      }
      vector.getOffsetBuffer().setInt((i + 1) * BaseRepeatedValueVector.OFFSET_WIDTH, curPos);
    }
    dataVector.setValueCount(curPos);
    vector.setLastSet(values.length - 1);
    vector.setValueCount(values.length);
  }

  /**
   * Populate values for {@link LargeListVector}.
   */
  public static void setVector(LargeListVector vector, List<Integer>... values) {
    vector.allocateNewSafe();
    Types.MinorType type = Types.MinorType.INT;
    vector.addOrGetVector(FieldType.nullable(type.getType()));

    IntVector dataVector = (IntVector) vector.getDataVector();
    dataVector.allocateNew();

    // set underlying vectors
    int curPos = 0;
    vector.getOffsetBuffer().setLong(0, curPos);
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        BitVectorHelper.unsetBit(vector.getValidityBuffer(), i);
      } else {
        BitVectorHelper.setBit(vector.getValidityBuffer(), i);
        for (int value : values[i]) {
          dataVector.setSafe(curPos, value);
          curPos += 1;
        }
      }
      vector.getOffsetBuffer().setLong((long) (i + 1) * LargeListVector.OFFSET_WIDTH, curPos);
    }
    dataVector.setValueCount(curPos);
    vector.setLastSet(values.length - 1);
    vector.setValueCount(values.length);
  }

  /**
   * Populate values for {@link FixedSizeListVector}.
   */
  public static void setVector(FixedSizeListVector vector, List<Integer>... values) {
    vector.allocateNewSafe();
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        assertEquals(vector.getListSize(), values[i].size());
      }
    }

    Types.MinorType type = Types.MinorType.INT;
    vector.addOrGetVector(FieldType.nullable(type.getType()));

    IntVector dataVector = (IntVector) vector.getDataVector();
    dataVector.allocateNew();

    // set underlying vectors
    int curPos = 0;
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        BitVectorHelper.unsetBit(vector.getValidityBuffer(), i);
      } else {
        BitVectorHelper.setBit(vector.getValidityBuffer(), i);
        for (int value : values[i]) {
          dataVector.setSafe(curPos, value);
          curPos += 1;
        }
      }
    }
    dataVector.setValueCount(curPos);
    vector.setValueCount(values.length);
  }

  /**
   * Populate values for {@link StructVector}.
   */
  public static void setVector(StructVector vector, Map<String, List<Integer>> values) {
    vector.allocateNewSafe();

    int valueCount = 0;
    for (final Entry<String, List<Integer>> entry : values.entrySet()) {
      // Add the child
      final IntVector child = vector.addOrGet(entry.getKey(),
          FieldType.nullable(MinorType.INT.getType()), IntVector.class);

      // Write the values to the child
      child.allocateNew();
      final List<Integer> v = entry.getValue();
      for (int i = 0; i < v.size(); i++) {
        if (v.get(i) != null) {
          child.set(i, v.get(i));
          vector.setIndexDefined(i);
        } else {
          child.setNull(i);
        }
      }
      valueCount = Math.max(valueCount, v.size());
    }
    vector.setValueCount(valueCount);
  }
}

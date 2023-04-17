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

package org.apache.arrow.vector;

import java.math.BigDecimal;
import java.nio.charset.Charset;

/**
 * Helper class to generate test data for Nullable fixed and variable
 * width scalar vectors. Previous implementations of java vector classes
 * provided generateTestData(now deprecated) API to populate the vector
 * with sample data. This class should be used for that purpose.
 */
public class GenerateSampleData {
  private GenerateSampleData() {}

  /** Populates <code>vector</code> with <code>valueCount</code> random values. */
  public static void generateTestData(final ValueVector vector, final int valueCount) {
    if (vector instanceof IntVector) {
      writeIntData((IntVector) vector, valueCount);
    } else if (vector instanceof DecimalVector) {
      writeDecimalData((DecimalVector) vector, valueCount);
    } else if (vector instanceof BitVector) {
      writeBooleanData((BitVector) vector, valueCount);
    } else if (vector instanceof VarCharVector) {
      writeVarCharData((VarCharVector) vector, valueCount);
    } else if (vector instanceof VarBinaryVector) {
      writeVarBinaryData((VarBinaryVector) vector, valueCount);
    } else if (vector instanceof BigIntVector) {
      writeBigIntData((BigIntVector) vector, valueCount);
    } else if (vector instanceof Float4Vector) {
      writeFloatData((Float4Vector) vector, valueCount);
    } else if (vector instanceof Float8Vector) {
      writeDoubleData((Float8Vector) vector, valueCount);
    } else if (vector instanceof DateDayVector) {
      writeDateDayData((DateDayVector) vector, valueCount);
    } else if (vector instanceof DateMilliVector) {
      writeDateMilliData((DateMilliVector) vector, valueCount);
    } else if (vector instanceof IntervalDayVector) {
      writeIntervalDayData((IntervalDayVector) vector, valueCount);
    } else if (vector instanceof IntervalYearVector) {
      writeIntervalYearData((IntervalYearVector) vector, valueCount);
    } else if (vector instanceof SmallIntVector) {
      writeSmallIntData((SmallIntVector) vector, valueCount);
    } else if (vector instanceof TinyIntVector) {
      writeTinyIntData((TinyIntVector) vector, valueCount);
    } else if (vector instanceof TimeMicroVector) {
      writeTimeMicroData((TimeMicroVector) vector, valueCount);
    } else if (vector instanceof TimeMilliVector) {
      writeTimeMilliData((TimeMilliVector) vector, valueCount);
    } else if (vector instanceof TimeNanoVector) {
      writeTimeNanoData((TimeNanoVector) vector, valueCount);
    } else if (vector instanceof TimeSecVector) {
      writeTimeSecData((TimeSecVector) vector, valueCount);
    } else if (vector instanceof TimeStampSecVector) {
      writeTimeStampData((TimeStampSecVector) vector, valueCount);
    } else if (vector instanceof TimeStampMicroVector) {
      writeTimeStampData((TimeStampMicroVector) vector, valueCount);
    } else if (vector instanceof TimeStampMilliVector) {
      writeTimeStampData((TimeStampMilliVector) vector, valueCount);
    } else if (vector instanceof TimeStampNanoVector) {
      writeTimeStampData((TimeStampNanoVector) vector, valueCount);
    } else if (vector instanceof TimeStampSecTZVector) {
      writeTimeStampData((TimeStampSecTZVector) vector, valueCount);
    } else if (vector instanceof TimeStampMicroTZVector) {
      writeTimeStampData((TimeStampMicroTZVector) vector, valueCount);
    } else if (vector instanceof TimeStampMilliTZVector) {
      writeTimeStampData((TimeStampMilliTZVector) vector, valueCount);
    } else if (vector instanceof TimeStampNanoTZVector) {
      writeTimeStampData((TimeStampNanoTZVector) vector, valueCount);
    } else if (vector instanceof UInt1Vector) {
      writeUInt1Data((UInt1Vector) vector, valueCount);
    } else if (vector instanceof UInt2Vector) {
      writeUInt2Data((UInt2Vector) vector, valueCount);
    } else if (vector instanceof UInt4Vector) {
      writeUInt4Data((UInt4Vector) vector, valueCount);
    } else if (vector instanceof UInt8Vector) {
      writeUInt8Data((UInt8Vector) vector, valueCount);
    }
  }

  private static void writeTimeStampData(TimeStampVector vector, int valueCount) {
    final long even = 100000;
    final long odd = 200000;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeDecimalData(DecimalVector vector, int valueCount) {
    final BigDecimal even = new BigDecimal(0.0543278923);
    final BigDecimal odd = new BigDecimal(2.0543278923);
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeIntData(IntVector vector, int valueCount) {
    final int even = 1000;
    final int odd = 2000;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeBooleanData(BitVector vector, int valueCount) {
    final int even = 0;
    final int odd = 1;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeIntervalYearData(IntervalYearVector vector, int valueCount) {
    final int even = 1;
    final int odd = 2;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeIntervalDayData(IntervalDayVector vector, int valueCount) {
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, 1, 50);
      } else {
        vector.setSafe(i, 2, 100);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeTimeSecData(TimeSecVector vector, int valueCount) {
    final int even = 500;
    final int odd = 900;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeTimeMilliData(TimeMilliVector vector, int valueCount) {
    final int even = 1000;
    final int odd = 2000;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeTimeMicroData(TimeMicroVector vector, int valueCount) {
    final long even = 1000000000;
    final long odd = 2000000000;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);

  }

  private static void writeTimeNanoData(TimeNanoVector vector, int valueCount) {
    final long even = 1000000000;
    final long odd = 2000000000;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeDateDayData(DateDayVector vector, int valueCount) {
    final int even = 1000;
    final int odd = 2000;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeDateMilliData(DateMilliVector vector, int valueCount) {
    final long even = 1000000000;
    final long odd = 2000000000;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeSmallIntData(SmallIntVector vector, int valueCount) {
    final short even = 10;
    final short odd = 20;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeTinyIntData(TinyIntVector vector, int valueCount) {
    final byte even = 1;
    final byte odd = 2;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeUInt1Data(UInt1Vector vector, int valueCount) {
    final byte even = 1;
    final byte odd = 2;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeUInt2Data(UInt2Vector vector, int valueCount) {
    final short even = 10;
    final short odd = 20;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeUInt4Data(UInt4Vector vector, int valueCount) {
    final int even = 1000;
    final int odd = 2000;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeUInt8Data(UInt8Vector vector, int valueCount) {
    final long even = 1000000000;
    final long odd = 2000000000;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeBigIntData(BigIntVector vector, int valueCount) {
    final long even = 1000000000;
    final long odd = 2000000000;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeFloatData(Float4Vector vector, int valueCount) {
    final float even = 20.3f;
    final float odd = 40.2f;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeDoubleData(Float8Vector vector, int valueCount) {
    final double even = 20.2373;
    final double odd = 40.2378;
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeVarBinaryData(VarBinaryVector vector, int valueCount) {
    Charset utf8Charset = Charset.forName("UTF-8");
    final byte[] even = "AAAAA1".getBytes(utf8Charset);
    final byte[] odd = "BBBBBBBBB2".getBytes(utf8Charset);
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeVarCharData(VarCharVector vector, int valueCount) {
    Charset utf8Charset = Charset.forName("UTF-8");
    final byte[] even = "AAAAA1".getBytes(utf8Charset);
    final byte[] odd = "BBBBBBBBB2".getBytes(utf8Charset);
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }
}


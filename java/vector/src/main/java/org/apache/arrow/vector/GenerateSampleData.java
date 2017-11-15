/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

  public static void generateTestData(final ValueVector vector, final int valueCount) {
    if (vector instanceof NullableIntVector) {
      writeIntData((NullableIntVector) vector, valueCount);
    } else if (vector instanceof NullableDecimalVector) {
      writeDecimalData((NullableDecimalVector) vector, valueCount);
    } else if (vector instanceof NullableBitVector) {
      writeBooleanData((NullableBitVector) vector, valueCount);
    } else if (vector instanceof NullableVarCharVector) {
      writeVarCharData((NullableVarCharVector) vector, valueCount);
    } else if (vector instanceof NullableVarBinaryVector) {
      writeVarBinaryData((NullableVarBinaryVector) vector, valueCount);
    } else if (vector instanceof NullableBigIntVector) {
      writeBigIntData((NullableBigIntVector) vector, valueCount);
    } else if (vector instanceof NullableFloat4Vector) {
      writeFloatData((NullableFloat4Vector) vector, valueCount);
    } else if (vector instanceof NullableFloat8Vector) {
      writeDoubleData((NullableFloat8Vector) vector, valueCount);
    } else if (vector instanceof NullableDateDayVector) {
      writeDateDayData((NullableDateDayVector) vector, valueCount);
    } else if (vector instanceof NullableDateMilliVector) {
      writeDateMilliData((NullableDateMilliVector) vector, valueCount);
    } else if (vector instanceof NullableIntervalDayVector) {
      writeIntervalDayData((NullableIntervalDayVector) vector, valueCount);
    } else if (vector instanceof NullableIntervalYearVector) {
      writeIntervalYearData((NullableIntervalYearVector) vector, valueCount);
    } else if (vector instanceof NullableSmallIntVector) {
      writeSmallIntData((NullableSmallIntVector) vector, valueCount);
    } else if (vector instanceof NullableTinyIntVector) {
      writeTinyIntData((NullableTinyIntVector) vector, valueCount);
    } else if (vector instanceof NullableTimeMicroVector) {
      writeTimeMicroData((NullableTimeMicroVector) vector, valueCount);
    } else if (vector instanceof NullableTimeMilliVector) {
      writeTimeMilliData((NullableTimeMilliVector) vector, valueCount);
    } else if (vector instanceof NullableTimeNanoVector) {
      writeTimeNanoData((NullableTimeNanoVector) vector, valueCount);
    } else if (vector instanceof NullableTimeSecVector) {
      writeTimeSecData((NullableTimeSecVector) vector, valueCount);
    } else if (vector instanceof NullableTimeStampSecVector) {
      writeTimeStampData((NullableTimeStampSecVector) vector, valueCount);
    } else if (vector instanceof NullableTimeStampMicroVector) {
      writeTimeStampData((NullableTimeStampMicroVector) vector, valueCount);
    } else if (vector instanceof NullableTimeStampMilliVector) {
      writeTimeStampData((NullableTimeStampMilliVector) vector, valueCount);
    } else if (vector instanceof NullableTimeStampNanoVector) {
      writeTimeStampData((NullableTimeStampNanoVector) vector, valueCount);
    } else if (vector instanceof NullableTimeStampSecTZVector) {
      writeTimeStampData((NullableTimeStampSecTZVector) vector, valueCount);
    } else if (vector instanceof NullableTimeStampMicroTZVector) {
      writeTimeStampData((NullableTimeStampMicroTZVector) vector, valueCount);
    } else if (vector instanceof NullableTimeStampMilliTZVector) {
      writeTimeStampData((NullableTimeStampMilliTZVector) vector, valueCount);
    } else if (vector instanceof NullableTimeStampNanoTZVector) {
      writeTimeStampData((NullableTimeStampNanoTZVector) vector, valueCount);
    }
  }

  private static void writeTimeStampData(NullableTimeStampVector vector, int valueCount) {
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

  private static void writeDecimalData(NullableDecimalVector vector, int valueCount) {
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

  private static void writeIntData(NullableIntVector vector, int valueCount) {
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

  private static void writeBooleanData(NullableBitVector vector, int valueCount) {
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

  private static void writeIntervalYearData(NullableIntervalYearVector vector, int valueCount) {
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

  private static void writeIntervalDayData(NullableIntervalDayVector vector, int valueCount) {
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, 1, 50);
      } else {
        vector.setSafe(i, 2, 100);
      }
    }
    vector.setValueCount(valueCount);
  }

  private static void writeTimeSecData(NullableTimeSecVector vector, int valueCount) {
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

  private static void writeTimeMilliData(NullableTimeMilliVector vector, int valueCount) {
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

  private static void writeTimeMicroData(NullableTimeMicroVector vector, int valueCount) {
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

  private static void writeTimeNanoData(NullableTimeNanoVector vector, int valueCount) {
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

  private static void writeDateDayData(NullableDateDayVector vector, int valueCount) {
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

  private static void writeDateMilliData(NullableDateMilliVector vector, int valueCount) {
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

  private static void writeSmallIntData(NullableSmallIntVector vector, int valueCount) {
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

  private static void writeTinyIntData(NullableTinyIntVector vector, int valueCount) {
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

  private static void writeBigIntData(NullableBigIntVector vector, int valueCount) {
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

  private static void writeFloatData(NullableFloat4Vector vector, int valueCount) {
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

  private static void writeDoubleData(NullableFloat8Vector vector, int valueCount) {
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

  private static void writeVarBinaryData(NullableVarBinaryVector vector, int valueCount) {
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

  private static void writeVarCharData(NullableVarCharVector vector, int valueCount) {
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


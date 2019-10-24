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

import org.apache.arrow.vector.holders.IntervalDayHolder;

/**
 * Utility for populating {@link org.apache.arrow.vector.ValueVector}
 */
public class ValueVectorDataPopulator {

  public static void setVector(BigIntVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(BitVector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(DateDayVector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(DateMilliVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(DecimalVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(DurationVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(FixedSizeBinaryVector vector, byte[]... values) {
    final int length = values.length;
    vector.allocateNewSafe();
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(Float4Vector vector, Float... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(Float8Vector vector, Double... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(IntVector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(IntervalDayVector vector, IntervalDayHolder... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i].days, values[i].milliseconds);
      }
    }
  }

  public static void setVector(IntervalYearVector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(SmallIntVector vector, Short... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TimeMicroVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TimeMilliVector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TimeNanoVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TimeSecVector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TimeStampMicroTZVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TimeStampMicroVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TimeStampMilliTZVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TimeStampMilliVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TimeStampNanoTZVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TimeStampNanoVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TimeStampSecTZVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TimeStampSecVector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(TinyIntVector vector, Byte... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(UInt1Vector vector, Byte... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(UInt2Vector vector, Character... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(UInt4Vector vector, Integer... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(UInt8Vector vector, Long... values) {
    final int length = values.length;
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(VarBinaryVector vector, byte[]... values) {
    final int length = values.length;
    vector.allocateNewSafe();
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

  public static void setVector(VarCharVector vector, byte[]... values) {
    final int length = values.length;
    vector.allocateNewSafe();
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
  }

}

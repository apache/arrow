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


package org.apache.arrow.driver.jdbc.accessor.impl.numeric;

import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.holders.NullableUInt1Holder;
import org.apache.arrow.vector.holders.NullableUInt2Holder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.holders.NullableUInt8Holder;

/**
 * A custom getter for values from the {@link BaseIntVector}.
 */
class ArrowFlightJdbcNumericGetter {
  /**
   * A holder for values from the {@link BaseIntVector}.
   */
  static class NumericHolder {
    int isSet; // Tells if value is set; 0 = not set, 1 = set
    long value; // Holds actual value
  }

  /**
   * Functional interface for a getter to baseInt values.
   */
  @FunctionalInterface
  interface Getter {
    void get(int index, NumericHolder holder);
  }

  /**
   * Main class that will check the type of the vector to create
   * a specific getter.
   *
   * @param vector an instance of the {@link BaseIntVector}
   * @return a getter.
   */
  static Getter createGetter(BaseIntVector vector) {
    if (vector instanceof UInt1Vector) {
      return createGetter((UInt1Vector) vector);
    } else if (vector instanceof UInt2Vector) {
      return createGetter((UInt2Vector) vector);
    } else if (vector instanceof UInt4Vector) {
      return createGetter((UInt4Vector) vector);
    } else if (vector instanceof UInt8Vector) {
      return createGetter((UInt8Vector) vector);
    } else if (vector instanceof TinyIntVector) {
      return createGetter((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      return createGetter((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      return createGetter((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      return createGetter((BigIntVector) vector);
    }

    throw new UnsupportedOperationException("No valid IntVector was provided.");
  }

  /**
   * Create a specific getter for {@link UInt1Vector}.
   *
   * @param vector an instance of the {@link UInt1Vector}
   * @return a getter.
   */
  private static Getter createGetter(UInt1Vector vector) {
    NullableUInt1Holder nullableUInt1Holder = new NullableUInt1Holder();

    return (index, holder) -> {
      vector.get(index, nullableUInt1Holder);

      holder.isSet = nullableUInt1Holder.isSet;
      holder.value = nullableUInt1Holder.value;
    };
  }

  /**
   * Create a specific getter for {@link UInt2Vector}.
   *
   * @param vector an instance of the {@link UInt2Vector}
   * @return a getter.
   */
  private static Getter createGetter(UInt2Vector vector) {
    NullableUInt2Holder nullableUInt2Holder = new NullableUInt2Holder();
    return (index, holder) -> {
      vector.get(index, nullableUInt2Holder);

      holder.isSet = nullableUInt2Holder.isSet;
      holder.value = nullableUInt2Holder.value;
    };
  }

  /**
   * Create a specific getter for {@link UInt4Vector}.
   *
   * @param vector an instance of the {@link UInt4Vector}
   * @return a getter.
   */
  private static Getter createGetter(UInt4Vector vector) {
    NullableUInt4Holder nullableUInt4Holder = new NullableUInt4Holder();
    return (index, holder) -> {
      vector.get(index, nullableUInt4Holder);

      holder.isSet = nullableUInt4Holder.isSet;
      holder.value = nullableUInt4Holder.value;
    };
  }

  /**
   * Create a specific getter for {@link UInt8Vector}.
   *
   * @param vector an instance of the {@link UInt8Vector}
   * @return a getter.
   */
  private static Getter createGetter(UInt8Vector vector) {
    NullableUInt8Holder nullableUInt8Holder = new NullableUInt8Holder();
    return (index, holder) -> {
      vector.get(index, nullableUInt8Holder);

      holder.isSet = nullableUInt8Holder.isSet;
      holder.value = nullableUInt8Holder.value;
    };
  }

  /**
   * Create a specific getter for {@link TinyIntVector}.
   *
   * @param vector an instance of the {@link TinyIntVector}
   * @return a getter.
   */
  private static Getter createGetter(TinyIntVector vector) {
    NullableTinyIntHolder nullableTinyIntHolder = new NullableTinyIntHolder();
    return (index, holder) -> {
      vector.get(index, nullableTinyIntHolder);

      holder.isSet = nullableTinyIntHolder.isSet;
      holder.value = nullableTinyIntHolder.value;
    };
  }

  /**
   * Create a specific getter for {@link SmallIntVector}.
   *
   * @param vector an instance of the {@link SmallIntVector}
   * @return a getter.
   */
  private static Getter createGetter(SmallIntVector vector) {
    NullableSmallIntHolder nullableSmallIntHolder = new NullableSmallIntHolder();
    return (index, holder) -> {
      vector.get(index, nullableSmallIntHolder);

      holder.isSet = nullableSmallIntHolder.isSet;
      holder.value = nullableSmallIntHolder.value;
    };
  }

  /**
   * Create a specific getter for {@link IntVector}.
   *
   * @param vector an instance of the {@link IntVector}
   * @return a getter.
   */
  private static Getter createGetter(IntVector vector) {
    NullableIntHolder nullableIntHolder = new NullableIntHolder();
    return (index, holder) -> {
      vector.get(index, nullableIntHolder);

      holder.isSet = nullableIntHolder.isSet;
      holder.value = nullableIntHolder.value;
    };
  }

  /**
   * Create a specific getter for {@link BigIntVector}.
   *
   * @param vector an instance of the {@link BigIntVector}
   * @return a getter.
   */
  private static Getter createGetter(BigIntVector vector) {
    NullableBigIntHolder nullableBigIntHolder = new NullableBigIntHolder();
    return (index, holder) -> {
      vector.get(index, nullableBigIntHolder);

      holder.isSet = nullableBigIntHolder.isSet;
      holder.value = nullableBigIntHolder.value;
    };
  }
}

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

import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;

/**
 * A custom getter for values from the {@link FloatingPointVector}.
 */
class ArrowFlightJdbcDecimalGetter {

  /**
   * A holder for values from the {@link FloatingPointVector}.
   */
  static class DecimalHolder {
    int isSet;
    double value;
  }

  /**
   * A holder for values from the {@link FloatingPointVector}.
   */
  interface Getter {
    void get(int index, DecimalHolder holder);
  }

  /**
   * Main class that will check the type of the vector to create
   * a specific getter.
   *
   * @param vector an instance of the {@link FloatingPointVector}
   *
   * @return a getter.
   */
  static Getter createGetter(FloatingPointVector vector) {
    if (vector instanceof Float4Vector) {
      return createGetter((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      return createGetter((Float8Vector) vector);
    }

    throw new UnsupportedOperationException();
  }

  /**
   * Create a specific getter for {@link Float4Vector}.
   *
   * @param vector an instance of the {@link Float4Vector}
   *
   * @return a getter.
   */
  private static Getter createGetter(Float4Vector vector) {
    NullableFloat4Holder nullableFloat4Holder = new NullableFloat4Holder();
    return (index, holder) -> {
      vector.get(index, nullableFloat4Holder);

      holder.isSet = nullableFloat4Holder.isSet;
      holder.value = nullableFloat4Holder.value;
    };
  }

  /**
   * Create a specific getter for {@link Float8Vector}.
   *
   * @param vector an instance of the {@link Float8Vector}
   *
   * @return a getter.
   */
  private static Getter createGetter(Float8Vector vector) {
    NullableFloat8Holder nullableFloat4Holder = new NullableFloat8Holder();
    return (index, holder) -> {
      vector.get(index, nullableFloat4Holder);

      holder.isSet = nullableFloat4Holder.isSet;
      holder.value = nullableFloat4Holder.value;
    };
  }
}

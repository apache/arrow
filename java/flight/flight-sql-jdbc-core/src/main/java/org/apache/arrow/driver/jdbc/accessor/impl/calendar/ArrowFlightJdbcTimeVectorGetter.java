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

package org.apache.arrow.driver.jdbc.accessor.impl.calendar;

import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.holders.NullableTimeMicroHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeNanoHolder;
import org.apache.arrow.vector.holders.NullableTimeSecHolder;

/**
 * Auxiliary class used to unify data access on Time*Vectors.
 */
final class ArrowFlightJdbcTimeVectorGetter {

  private ArrowFlightJdbcTimeVectorGetter() {
    // Prevent instantiation.
  }

  /**
   * Auxiliary class meant to unify TimeStamp*Vector#get implementations with different classes of ValueHolders.
   */
  static class Holder {
    int isSet; // Tells if value is set; 0 = not set, 1 = set
    long value; // Holds actual value in its respective timeunit
  }

  /**
   * Functional interface used to unify TimeStamp*Vector#get implementations.
   */
  @FunctionalInterface
  interface Getter {
    void get(int index, Holder holder);
  }

  static Getter createGetter(TimeNanoVector vector) {
    NullableTimeNanoHolder auxHolder = new NullableTimeNanoHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }

  static Getter createGetter(TimeMicroVector vector) {
    NullableTimeMicroHolder auxHolder = new NullableTimeMicroHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }

  static Getter createGetter(TimeMilliVector vector) {
    NullableTimeMilliHolder auxHolder = new NullableTimeMilliHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }

  static Getter createGetter(TimeSecVector vector) {
    NullableTimeSecHolder auxHolder = new NullableTimeSecHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }
}

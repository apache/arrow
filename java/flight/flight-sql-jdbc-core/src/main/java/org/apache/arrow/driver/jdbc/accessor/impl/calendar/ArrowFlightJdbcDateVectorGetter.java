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

import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;

/**
 * Auxiliary class used to unify data access on TimeStampVectors.
 */
final class ArrowFlightJdbcDateVectorGetter {

  private ArrowFlightJdbcDateVectorGetter() {
    // Prevent instantiation.
  }

  /**
   * Auxiliary class meant to unify Date*Vector#get implementations with different classes of ValueHolders.
   */
  static class Holder {
    int isSet; // Tells if value is set; 0 = not set, 1 = set
    long value; // Holds actual value in its respective timeunit
  }

  /**
   * Functional interface used to unify Date*Vector#get implementations.
   */
  @FunctionalInterface
  interface Getter {
    void get(int index, Holder holder);
  }

  static Getter createGetter(DateDayVector vector) {
    NullableDateDayHolder auxHolder = new NullableDateDayHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }

  static Getter createGetter(DateMilliVector vector) {
    NullableDateMilliHolder auxHolder = new NullableDateMilliHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }
}

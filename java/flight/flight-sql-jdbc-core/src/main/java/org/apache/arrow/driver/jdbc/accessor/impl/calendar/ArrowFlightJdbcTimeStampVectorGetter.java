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

import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.holders.NullableTimeStampMicroHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMicroTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampNanoHolder;
import org.apache.arrow.vector.holders.NullableTimeStampNanoTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampSecHolder;
import org.apache.arrow.vector.holders.NullableTimeStampSecTZHolder;

/**
 * Auxiliary class used to unify data access on TimeStampVectors.
 */
final class ArrowFlightJdbcTimeStampVectorGetter {

  private ArrowFlightJdbcTimeStampVectorGetter() {
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

  static Getter createGetter(TimeStampVector vector) {
    if (vector instanceof TimeStampNanoVector) {
      return createGetter((TimeStampNanoVector) vector);
    } else if (vector instanceof TimeStampNanoTZVector) {
      return createGetter((TimeStampNanoTZVector) vector);
    } else if (vector instanceof TimeStampMicroVector) {
      return createGetter((TimeStampMicroVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      return createGetter((TimeStampMicroTZVector) vector);
    } else if (vector instanceof TimeStampMilliVector) {
      return createGetter((TimeStampMilliVector) vector);
    } else if (vector instanceof TimeStampMilliTZVector) {
      return createGetter((TimeStampMilliTZVector) vector);
    } else if (vector instanceof TimeStampSecVector) {
      return createGetter((TimeStampSecVector) vector);
    } else if (vector instanceof TimeStampSecTZVector) {
      return createGetter((TimeStampSecTZVector) vector);
    }

    throw new UnsupportedOperationException("Unsupported Timestamp vector type");
  }

  private static Getter createGetter(TimeStampNanoVector vector) {
    NullableTimeStampNanoHolder auxHolder = new NullableTimeStampNanoHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }

  private static Getter createGetter(TimeStampNanoTZVector vector) {
    NullableTimeStampNanoTZHolder auxHolder = new NullableTimeStampNanoTZHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }

  private static Getter createGetter(TimeStampMicroVector vector) {
    NullableTimeStampMicroHolder auxHolder = new NullableTimeStampMicroHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }

  private static Getter createGetter(TimeStampMicroTZVector vector) {
    NullableTimeStampMicroTZHolder auxHolder = new NullableTimeStampMicroTZHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }

  private static Getter createGetter(TimeStampMilliVector vector) {
    NullableTimeStampMilliHolder auxHolder = new NullableTimeStampMilliHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }

  private static Getter createGetter(TimeStampMilliTZVector vector) {
    NullableTimeStampMilliTZHolder auxHolder = new NullableTimeStampMilliTZHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }

  private static Getter createGetter(TimeStampSecVector vector) {
    NullableTimeStampSecHolder auxHolder = new NullableTimeStampSecHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }

  private static Getter createGetter(TimeStampSecTZVector vector) {
    NullableTimeStampSecTZHolder auxHolder = new NullableTimeStampSecTZHolder();
    return (index, holder) -> {
      vector.get(index, auxHolder);
      holder.isSet = auxHolder.isSet;
      holder.value = auxHolder.value;
    };
  }
}

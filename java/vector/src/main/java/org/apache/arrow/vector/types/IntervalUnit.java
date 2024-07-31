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
package org.apache.arrow.vector.types;

/** Resolutions for Interval Vectors. */
public enum IntervalUnit {
  /**
   * Values are stored as number of months (which can be converted into years and months via
   * division).
   */
  YEAR_MONTH(org.apache.arrow.flatbuf.IntervalUnit.YEAR_MONTH),
  /** Values are stored as some number of days and some number of milliseconds within that day. */
  DAY_TIME(org.apache.arrow.flatbuf.IntervalUnit.DAY_TIME),
  /** Values are stored as number of months, days and nanoseconds. */
  MONTH_DAY_NANO(org.apache.arrow.flatbuf.IntervalUnit.MONTH_DAY_NANO);

  private static final IntervalUnit[] valuesByFlatbufId =
      new IntervalUnit[IntervalUnit.values().length];

  static {
    for (IntervalUnit v : IntervalUnit.values()) {
      valuesByFlatbufId[v.flatbufID] = v;
    }
  }

  private final short flatbufID;

  private IntervalUnit(short flatbufID) {
    this.flatbufID = flatbufID;
  }

  public short getFlatbufID() {
    return flatbufID;
  }

  public static IntervalUnit fromFlatbufID(short id) {
    return valuesByFlatbufId[id];
  }
}

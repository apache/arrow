/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.apache.arrow.vector.types;

public enum TimeUnit {
  SECOND(org.apache.arrow.flatbuf.TimeUnit.SECOND, java.util.concurrent.TimeUnit.SECONDS),
  MILLISECOND(org.apache.arrow.flatbuf.TimeUnit.MILLISECOND, java.util.concurrent.TimeUnit.MILLISECONDS),
  MICROSECOND(org.apache.arrow.flatbuf.TimeUnit.MICROSECOND, java.util.concurrent.TimeUnit.MICROSECONDS),
  NANOSECOND(org.apache.arrow.flatbuf.TimeUnit.NANOSECOND, java.util.concurrent.TimeUnit.NANOSECONDS);

  private static final TimeUnit[] valuesByFlatbufId = new TimeUnit[TimeUnit.values().length];

  static {
    for (TimeUnit v : TimeUnit.values()) {
      valuesByFlatbufId[v.flatbufID] = v;
    }
  }

  private final short flatbufID;
  private final java.util.concurrent.TimeUnit timeUnit;

  TimeUnit(short flatbufID, java.util.concurrent.TimeUnit timeUnit) {
    this.flatbufID = flatbufID;
    this.timeUnit = timeUnit;
  }

  public short getFlatbufID() {
    return flatbufID;
  }

  public static TimeUnit fromFlatbufID(short id) {
    return valuesByFlatbufId[id];
  }

  public final long toNanos(long duration) {
    return timeUnit.toNanos(duration);
  }

  public final long toMicros(long duration) {
    return timeUnit.toMicros(duration);
  }

  public final long toMillis(long duration) {
    return timeUnit.toMillis(duration);
  }

  public final long toSeconds(long duration) {
    return timeUnit.toSeconds(duration);
  }
}

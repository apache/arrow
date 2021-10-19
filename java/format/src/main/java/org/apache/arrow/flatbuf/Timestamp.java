// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// 
// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
/**
 * Time elapsed from the Unix epoch, 00:00:00.000 on 1 January 1970, excluding
 * leap seconds, as a 64-bit integer. Note that UNIX time does not include
 * leap seconds.
 *
 * Date & time libraries often have multiple different data types for temporal
 * data.  In order to ease interoperability between different implementations the
 * Arrow project has some recommendations for encoding these types into a Timestamp
 * column.
 *
 * An "instant" represents a single moment in time that has no meaningful time zone
 * or the time zone is unknown.  A column of instants can also contain values from
 * multiple time zones.  To encode an instant set the timezone string to "UTC".
 *
 * A "zoned date-time" represents a single moment in time that has a meaningful
 * reference time zone.  To encode a zoned date-time as a Timestamp set the timezone
 * string to the name of the timezone.  There is some ambiguity between an instant
 * and a zoned date-time with the UTC time zone.  Both of these are stored the same.
 * Typically, this distinction does not matter.  If it does, then an application should
 * use custom metadata or an extension type to distinguish between the two cases.
 *
 * An "offset date-time" represents a single moment in time combined with a meaningful
 * offset from UTC.  To encode an offset date-time as a Timestamp set the timezone string
 * to the numeric time zone offset string (e.g. "+03:00").
 *
 * A "local date-time" does not represent a single moment in time.  It represents a wall
 * clock time combined with a date.  Because of daylight savings time there may multiple
 * instants that correspond to a single local date-time in any given time zone.  A
 * local date-time is often stored as a struct or a Date32/Time64 pair.  However, it can
 * also be encoded into a Timestamp column.  To do so the value should be the the time
 * elapsed from the Unix epoch so that a wall clock in UTC would display the desired time.
 * The timezone string should be set to null or the empty string.
 */
public final class Timestamp extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_1_12_0(); }
  public static Timestamp getRootAsTimestamp(ByteBuffer _bb) { return getRootAsTimestamp(_bb, new Timestamp()); }
  public static Timestamp getRootAsTimestamp(ByteBuffer _bb, Timestamp obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Timestamp __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public short unit() { int o = __offset(4); return o != 0 ? bb.getShort(o + bb_pos) : 0; }
  /**
   * The time zone is a string indicating the name of a time zone, one of:
   *
   * * As used in the Olson time zone database (the "tz database" or
   *   "tzdata"), such as "America/New_York"
   * * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
   *
   * Whether a timezone string is present indicates different semantics about
   * the data:
   *
   * * If the time zone is null or an empty string, the data is a local date-time
   *   and does not represent a single moment in time.  Instead it represents a wall clock
   *   time and care should be taken to avoid interpreting it semantically as an instant.
   *
   * * If the time zone is set to a valid value, values can be displayed as
   *   "localized" to that time zone, even though the underlying 64-bit
   *   integers are identical to the same data stored in UTC. Converting
   *   between time zones is a metadata-only operation and does not change the
   *   underlying values
   */
  public String timezone() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer timezoneAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public ByteBuffer timezoneInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 6, 1); }

  public static int createTimestamp(FlatBufferBuilder builder,
      short unit,
      int timezoneOffset) {
    builder.startTable(2);
    Timestamp.addTimezone(builder, timezoneOffset);
    Timestamp.addUnit(builder, unit);
    return Timestamp.endTimestamp(builder);
  }

  public static void startTimestamp(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addUnit(FlatBufferBuilder builder, short unit) { builder.addShort(0, unit, 0); }
  public static void addTimezone(FlatBufferBuilder builder, int timezoneOffset) { builder.addOffset(1, timezoneOffset, 0); }
  public static int endTimestamp(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Timestamp get(int j) { return get(new Timestamp(), j); }
    public Timestamp get(Timestamp obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}


/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.types;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Types {
  public enum MinorType {
    LATE,   //  late binding type
    MAP,   //  an empty map column.  Useful for conceptual setup.  Children listed within here

    TINYINT,   //  single byte signed integer
    SMALLINT,   //  two byte signed integer
    INT,   //  four byte signed integer
    BIGINT,   //  eight byte signed integer
    DECIMAL9,   //  a decimal supporting precision between 1 and 9
    DECIMAL18,   //  a decimal supporting precision between 10 and 18
    DECIMAL28SPARSE,   //  a decimal supporting precision between 19 and 28
    DECIMAL38SPARSE,   //  a decimal supporting precision between 29 and 38
    MONEY,   //  signed decimal with two digit precision
    DATE,   //  days since 4713bc
    TIME,   //  time in micros before or after 2000/1/1
    TIMETZ,  //  time in micros before or after 2000/1/1 with timezone
    TIMESTAMPTZ,   //  unix epoch time in millis
    TIMESTAMP,   //  TBD
    INTERVAL,   //  TBD
    FLOAT4,   //  4 byte ieee 754
    FLOAT8,   //  8 byte ieee 754
    BIT,  //  single bit value (boolean)
    FIXEDCHAR,  //  utf8 fixed length string, padded with spaces
    FIXED16CHAR,
    FIXEDBINARY,   //  fixed length binary, padded with 0 bytes
    VARCHAR,   //  utf8 variable length string
    VAR16CHAR, // utf16 variable length string
    VARBINARY,   //  variable length binary
    UINT1,  //  unsigned 1 byte integer
    UINT2,  //  unsigned 2 byte integer
    UINT4,   //  unsigned 4 byte integer
    UINT8,   //  unsigned 8 byte integer
    DECIMAL28DENSE, // dense decimal representation, supporting precision between 19 and 28
    DECIMAL38DENSE, // dense decimal representation, supporting precision between 28 and 38
    NULL, // a value of unknown type (e.g. a missing reference).
    INTERVALYEAR, // Interval type specifying YEAR to MONTH
    INTERVALDAY, // Interval type specifying DAY to SECONDS
    LIST,
    GENERIC_OBJECT,
    UNION
  }

  public enum DataMode {
    REQUIRED,
    OPTIONAL,
    REPEATED
  }

  public static class MajorType {
    private MinorType minorType;
    private DataMode mode;
    private int precision;
    private int scale;
    private int timezone;
    private int width;
    private List<MinorType> subTypes;

    public MajorType(MinorType minorType, DataMode mode) {
      this(minorType, mode, 0, 0, 0, 0, null);
    }

    public MajorType(MinorType minorType, DataMode mode, int precision, int scale) {
      this(minorType, mode, precision, scale, 0, 0, null);
    }

    public MajorType(MinorType minorType, DataMode mode, int precision, int scale, int timezone, List<MinorType> subTypes) {
      this(minorType, mode, precision, scale, timezone, 0, subTypes);
    }

    public MajorType(MinorType minorType, DataMode mode, int precision, int scale, int timezone, int width, List<MinorType> subTypes) {
      this.minorType = minorType;
      this.mode = mode;
      this.precision = precision;
      this.scale = scale;
      this.timezone = timezone;
      this.width = width;
      this.subTypes = subTypes;
      if (subTypes == null) {
        this.subTypes = new ArrayList<>();
      }
    }

    public MinorType getMinorType() {
      return minorType;
    }

    public DataMode getMode() {
      return mode;
    }

    public int getPrecision() {
      return precision;
    }

    public int getScale() {
      return scale;
    }

    public int getTimezone() {
      return timezone;
    }

    public List<MinorType> getSubTypes() {
      return subTypes;
    }

    public int getWidth() {
      return width;
    }


    @Override
    public boolean equals(Object other) {
      if (other == null) {
        return false;
      }
      if (!(other instanceof MajorType)) {
        return false;
      }
      MajorType that = (MajorType) other;
      return this.minorType == that.minorType &&
              this.mode == that.mode &&
              this.precision == that.precision &&
              this.scale == that.scale &&
              this.timezone == that.timezone &&
              this.width == that.width &&
              Objects.equals(this.subTypes, that.subTypes);
    }

  }

  public static MajorType required(MinorType minorType) {
    return new MajorType(minorType, DataMode.REQUIRED);
  }
  public static MajorType optional(MinorType minorType) {
    return new MajorType(minorType, DataMode.OPTIONAL);
  }
  public static MajorType repeated(MinorType minorType) {
    return new MajorType(minorType, DataMode.REPEATED);
  }
}

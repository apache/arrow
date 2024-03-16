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

package org.apache.arrow.c;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;

/**
 * Conversion between {@link ArrowType} and string formats, as per C data
 * interface specification.
 */
final class Format {

  private Format() {
  }

  static String asString(ArrowType arrowType) {
    if (arrowType instanceof ExtensionType) {
      ArrowType innerType = ((ExtensionType) arrowType).storageType();
      return asString(innerType);
    }

    switch (arrowType.getTypeID()) {
      case Binary:
        return "z";
      case Bool:
        return "b";
      case Date: {
        ArrowType.Date type = (ArrowType.Date) arrowType;
        switch (type.getUnit()) {
          case DAY:
            return "tdD";
          case MILLISECOND:
            return "tdm";
          default:
            throw new UnsupportedOperationException(
                String.format("Date type with unit %s is unsupported", type.getUnit()));
        }
      }
      case Decimal: {
        ArrowType.Decimal type = (ArrowType.Decimal) arrowType;
        if (type.getBitWidth() == 128) {
          return String.format("d:%d,%d", type.getPrecision(), type.getScale());
        }
        return String.format("d:%d,%d,%d", type.getPrecision(), type.getScale(), type.getBitWidth());
      }
      case Duration: {
        ArrowType.Duration type = (ArrowType.Duration) arrowType;
        switch (type.getUnit()) {
          case SECOND:
            return "tDs";
          case MILLISECOND:
            return "tDm";
          case MICROSECOND:
            return "tDu";
          case NANOSECOND:
            return "tDn";
          default:
            throw new UnsupportedOperationException(
                String.format("Duration type with unit %s is unsupported", type.getUnit()));
        }
      }
      case FixedSizeBinary: {
        ArrowType.FixedSizeBinary type = (ArrowType.FixedSizeBinary) arrowType;
        return String.format("w:%d", type.getByteWidth());
      }
      case FixedSizeList: {
        ArrowType.FixedSizeList type = (ArrowType.FixedSizeList) arrowType;
        return String.format("+w:%d", type.getListSize());
      }
      case FloatingPoint: {
        ArrowType.FloatingPoint type = (ArrowType.FloatingPoint) arrowType;
        switch (type.getPrecision()) {
          case HALF:
            return "e";
          case SINGLE:
            return "f";
          case DOUBLE:
            return "g";
          default:
            throw new UnsupportedOperationException(
                String.format("FloatingPoint type with precision %s is unsupported", type.getPrecision()));
        }
      }
      case Int: {
        String format;
        ArrowType.Int type = (ArrowType.Int) arrowType;
        switch (type.getBitWidth()) {
          case Byte.SIZE:
            format = "C";
            break;
          case Short.SIZE:
            format = "S";
            break;
          case Integer.SIZE:
            format = "I";
            break;
          case Long.SIZE:
            format = "L";
            break;
          default:
            throw new UnsupportedOperationException(
                String.format("Int type with bitwidth %d is unsupported", type.getBitWidth()));
        }
        if (type.getIsSigned()) {
          format = format.toLowerCase();
        }
        return format;
      }
      case Interval: {
        ArrowType.Interval type = (ArrowType.Interval) arrowType;
        switch (type.getUnit()) {
          case DAY_TIME:
            return "tiD";
          case YEAR_MONTH:
            return "tiM";
          case MONTH_DAY_NANO:
            return "tin";
          default:
            throw new UnsupportedOperationException(
                String.format("Interval type with unit %s is unsupported", type.getUnit()));
        }
      }
      case LargeBinary:
        return "Z";
      case LargeList:
        return "+L";
      case LargeUtf8:
        return "U";
      case List:
        return "+l";
      case Map:
        return "+m";
      case Null:
        return "n";
      case Struct:
        return "+s";
      case Time: {
        ArrowType.Time type = (ArrowType.Time) arrowType;
        if (type.getUnit() == TimeUnit.SECOND && type.getBitWidth() == 32) {
          return "tts";
        } else if (type.getUnit() == TimeUnit.MILLISECOND && type.getBitWidth() == 32) {
          return "ttm";
        } else if (type.getUnit() == TimeUnit.MICROSECOND && type.getBitWidth() == 64) {
          return "ttu";
        } else if (type.getUnit() == TimeUnit.NANOSECOND && type.getBitWidth() == 64) {
          return "ttn";
        } else {
          throw new UnsupportedOperationException(String.format("Time type with unit %s and bitwidth %d is unsupported",
              type.getUnit(), type.getBitWidth()));
        }
      }
      case Timestamp: {
        String format;
        ArrowType.Timestamp type = (ArrowType.Timestamp) arrowType;
        switch (type.getUnit()) {
          case SECOND:
            format = "tss";
            break;
          case MILLISECOND:
            format = "tsm";
            break;
          case MICROSECOND:
            format = "tsu";
            break;
          case NANOSECOND:
            format = "tsn";
            break;
          default:
            throw new UnsupportedOperationException(
                String.format("Timestamp type with unit %s is unsupported", type.getUnit()));
        }
        String timezone = type.getTimezone();
        return String.format("%s:%s", format, timezone == null ? "" : timezone);
      }
      case Union:
        ArrowType.Union type = (ArrowType.Union) arrowType;
        String typeIDs = Arrays.stream(type.getTypeIds()).mapToObj(String::valueOf).collect(Collectors.joining(","));
        switch (type.getMode()) {
          case Dense:
            return String.format("+ud:%s", typeIDs);
          case Sparse:
            return String.format("+us:%s", typeIDs);
          default:
            throw new UnsupportedOperationException(
                String.format("Union type with mode %s is unsupported", type.getMode()));
        }
      case Utf8:
        return "u";
      case NONE:
        throw new IllegalArgumentException("Arrow type ID is NONE");
      default:
        throw new UnsupportedOperationException(String.format("Unknown type id %s", arrowType.getTypeID()));
    }
  }

  static ArrowType asType(String format, long flags)
      throws NumberFormatException, UnsupportedOperationException, IllegalStateException {
    switch (format) {
      case "n":
        return new ArrowType.Null();
      case "b":
        return new ArrowType.Bool();
      case "c":
        return new ArrowType.Int(8, true);
      case "C":
        return new ArrowType.Int(8, false);
      case "s":
        return new ArrowType.Int(16, true);
      case "S":
        return new ArrowType.Int(16, false);
      case "i":
        return new ArrowType.Int(32, true);
      case "I":
        return new ArrowType.Int(32, false);
      case "l":
        return new ArrowType.Int(64, true);
      case "L":
        return new ArrowType.Int(64, false);
      case "e":
        return new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
      case "f":
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case "g":
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case "z":
        return new ArrowType.Binary();
      case "Z":
        return new ArrowType.LargeBinary();
      case "u":
        return new ArrowType.Utf8();
      case "U":
        return new ArrowType.LargeUtf8();
      case "tdD":
        return new ArrowType.Date(DateUnit.DAY);
      case "tdm":
        return new ArrowType.Date(DateUnit.MILLISECOND);
      case "tts":
        return new ArrowType.Time(TimeUnit.SECOND, 32);
      case "ttm":
        return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
      case "ttu":
        return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
      case "ttn":
        return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
      case "tDs":
        return new ArrowType.Duration(TimeUnit.SECOND);
      case "tDm":
        return new ArrowType.Duration(TimeUnit.MILLISECOND);
      case "tDu":
        return new ArrowType.Duration(TimeUnit.MICROSECOND);
      case "tDn":
        return new ArrowType.Duration(TimeUnit.NANOSECOND);
      case "tiM":
        return new ArrowType.Interval(IntervalUnit.YEAR_MONTH);
      case "tiD":
        return new ArrowType.Interval(IntervalUnit.DAY_TIME);
      case "tin":
        return new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO);
      case "+l":
        return new ArrowType.List();
      case "+L":
        return new ArrowType.LargeList();
      case "+s":
        return new ArrowType.Struct();
      case "+m":
        boolean keysSorted = (flags & Flags.ARROW_FLAG_MAP_KEYS_SORTED) != 0;
        return new ArrowType.Map(keysSorted);
      default:
        String[] parts = format.split(":", 2);
        if (parts.length == 2) {
          return parseComplexFormat(parts[0], parts[1]);
        }
        throw new UnsupportedOperationException(String.format("Format %s is not supported", format));
    }
  }

  private static ArrowType parseComplexFormat(String format, String payload)
      throws NumberFormatException, UnsupportedOperationException, IllegalStateException {
    switch (format) {
      case "d": {
        int[] parts = payloadToIntArray(payload);
        Preconditions.checkState(parts.length == 2 || parts.length == 3, "Format %s:%s is illegal", format, payload);
        int precision = parts[0];
        int scale = parts[1];
        Integer bitWidth = (parts.length == 3) ? parts[2] : null;
        return ArrowType.Decimal.createDecimal(precision, scale, bitWidth);
      }
      case "w":
        return new ArrowType.FixedSizeBinary(Integer.parseInt(payload));
      case "+w":
        return new ArrowType.FixedSizeList(Integer.parseInt(payload));
      case "+ud":
        return new ArrowType.Union(UnionMode.Dense, payloadToIntArray(payload));
      case "+us":
        return new ArrowType.Union(UnionMode.Sparse, payloadToIntArray(payload));
      case "tss":
        return new ArrowType.Timestamp(TimeUnit.SECOND, payloadToTimezone(payload));
      case "tsm":
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, payloadToTimezone(payload));
      case "tsu":
        return new ArrowType.Timestamp(TimeUnit.MICROSECOND, payloadToTimezone(payload));
      case "tsn":
        return new ArrowType.Timestamp(TimeUnit.NANOSECOND, payloadToTimezone(payload));
      default:
        throw new UnsupportedOperationException(String.format("Format %s:%s is not supported", format, payload));
    }
  }

  private static int[] payloadToIntArray(String payload) throws NumberFormatException {
    return Arrays.stream(payload.split(",")).mapToInt(Integer::parseInt).toArray();
  }

  private static String payloadToTimezone(String payload) {
    if (payload.isEmpty()) {
      return null;
    }
    return payload;
  }
}

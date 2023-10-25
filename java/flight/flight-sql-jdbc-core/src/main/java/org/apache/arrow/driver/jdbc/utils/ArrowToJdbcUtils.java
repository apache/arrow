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

package org.apache.arrow.driver.jdbc.utils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Helper class to convert Arrow types to JDBC required values.
 */
class ArrowToJdbcUtils {
  static boolean isSigned(ArrowType type) {
    switch (type.getTypeID()) {
      case Int:
        return ((ArrowType.Int) type).getIsSigned();
      case FloatingPoint:
      case Decimal:
      case Interval:
      case Duration:
        return true;
      default:
        return false;
    }
  }

  static int toJdbcType(ArrowType type) {
    switch (type.getTypeID()) {
      case Null:
        return Types.NULL;
      case Struct:
        return Types.STRUCT;
      case List:
      case LargeList:
      case FixedSizeList:
        return Types.ARRAY;
      case Int:
        switch (((ArrowType.Int) type).getBitWidth()) {
          case 64:
            return Types.BIGINT;
          case 32:
            return Types.INTEGER;
          case 16:
            return Types.SMALLINT;
          case 8:
            return Types.TINYINT;
          default:
            // FIXME: Does this ever even happen?
            return Types.OTHER;
        }
      case FloatingPoint:
        /*
         * These are a bit confusing. The following are equivalent:
         *
         * Highest precision:
         * - JDBC REAL
         * - Arrow DOUBLE
         *
         * Lower precision:
         * - JDBC FLOAT
         * - JDBC DOUBLE
         * - Arrow SINGLE
         * - Arrow HALF
         */
        return ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.DOUBLE ?
                Types.REAL : Types.DOUBLE;
      case Utf8:
      case LargeUtf8:
        return Types.VARCHAR;
      case Binary:
      case LargeBinary:
        return Types.VARBINARY;
      case FixedSizeBinary:
        return Types.BINARY;
      case Bool:
        return Types.BOOLEAN;
      case Decimal:
        return Types.DECIMAL;
      case Date:
        return Types.DATE;
      case Time:
        return Types.TIME;
      case Timestamp:
        return Types.TIMESTAMP;
      case Interval:
      case Duration:
      default:
        return Types.OTHER;
    }

  }

  static String getClassName(ArrowType type) {
    switch (type.getTypeID()) {
      case List:
      case LargeList:
      case FixedSizeList:
        return ArrayList.class.getCanonicalName();
      case Map:
        return HashMap.class.getCanonicalName();
      case Int:
        return ((ArrowType.Int) type).getBitWidth() == 64 ?
                long.class.getCanonicalName() : int.class.getCanonicalName();
      case FloatingPoint:
        return ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.DOUBLE ?
                double.class.getCanonicalName() : float.class.getCanonicalName();
      case Utf8:
      case LargeUtf8:
        return String.class.getCanonicalName();
      case Binary:
      case LargeBinary:
      case FixedSizeBinary:
        return byte[].class.getCanonicalName();
      case Bool:
        return boolean.class.getCanonicalName();
      case Decimal:
        return BigDecimal.class.getCanonicalName();
      case Date:
        return Date.class.getCanonicalName();
      case Time:
        return Time.class.getCanonicalName();
      case Timestamp:
      case Interval:
      case Duration:
        return Timestamp.class.getCanonicalName();
      default:
        return null;
    }
  }
}

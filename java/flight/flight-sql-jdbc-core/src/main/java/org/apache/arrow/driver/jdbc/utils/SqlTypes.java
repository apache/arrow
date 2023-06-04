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

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * SQL Types utility functions.
 */
public class SqlTypes {
  private static final Map<Integer, String> typeIdToName = new HashMap<>();

  static {
    typeIdToName.put(Types.BIT, "BIT");
    typeIdToName.put(Types.TINYINT, "TINYINT");
    typeIdToName.put(Types.SMALLINT, "SMALLINT");
    typeIdToName.put(Types.INTEGER, "INTEGER");
    typeIdToName.put(Types.BIGINT, "BIGINT");
    typeIdToName.put(Types.FLOAT, "FLOAT");
    typeIdToName.put(Types.REAL, "REAL");
    typeIdToName.put(Types.DOUBLE, "DOUBLE");
    typeIdToName.put(Types.NUMERIC, "NUMERIC");
    typeIdToName.put(Types.DECIMAL, "DECIMAL");
    typeIdToName.put(Types.CHAR, "CHAR");
    typeIdToName.put(Types.VARCHAR, "VARCHAR");
    typeIdToName.put(Types.LONGVARCHAR, "LONGVARCHAR");
    typeIdToName.put(Types.DATE, "DATE");
    typeIdToName.put(Types.TIME, "TIME");
    typeIdToName.put(Types.TIMESTAMP, "TIMESTAMP");
    typeIdToName.put(Types.BINARY, "BINARY");
    typeIdToName.put(Types.VARBINARY, "VARBINARY");
    typeIdToName.put(Types.LONGVARBINARY, "LONGVARBINARY");
    typeIdToName.put(Types.NULL, "NULL");
    typeIdToName.put(Types.OTHER, "OTHER");
    typeIdToName.put(Types.JAVA_OBJECT, "JAVA_OBJECT");
    typeIdToName.put(Types.DISTINCT, "DISTINCT");
    typeIdToName.put(Types.STRUCT, "STRUCT");
    typeIdToName.put(Types.ARRAY, "ARRAY");
    typeIdToName.put(Types.BLOB, "BLOB");
    typeIdToName.put(Types.CLOB, "CLOB");
    typeIdToName.put(Types.REF, "REF");
    typeIdToName.put(Types.DATALINK, "DATALINK");
    typeIdToName.put(Types.BOOLEAN, "BOOLEAN");
    typeIdToName.put(Types.ROWID, "ROWID");
    typeIdToName.put(Types.NCHAR, "NCHAR");
    typeIdToName.put(Types.NVARCHAR, "NVARCHAR");
    typeIdToName.put(Types.LONGNVARCHAR, "LONGNVARCHAR");
    typeIdToName.put(Types.NCLOB, "NCLOB");
    typeIdToName.put(Types.SQLXML, "SQLXML");
    typeIdToName.put(Types.REF_CURSOR, "REF_CURSOR");
    typeIdToName.put(Types.TIME_WITH_TIMEZONE, "TIME_WITH_TIMEZONE");
    typeIdToName.put(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP_WITH_TIMEZONE");
  }

  /**
   * Convert given {@link ArrowType} to its corresponding SQL type name.
   *
   * @param arrowType type to convert from
   * @return corresponding SQL type name.
   * @see java.sql.Types
   */
  public static String getSqlTypeNameFromArrowType(ArrowType arrowType) {
    final int typeId = getSqlTypeIdFromArrowType(arrowType);
    return typeIdToName.get(typeId);
  }


  /**
   * Convert given {@link ArrowType} to its corresponding SQL type ID.
   *
   * @param arrowType type to convert from
   * @return corresponding SQL type ID.
   * @see java.sql.Types
   */
  public static int getSqlTypeIdFromArrowType(ArrowType arrowType) {
    final ArrowType.ArrowTypeID typeID = arrowType.getTypeID();
    switch (typeID) {
      case Int:
        final int bitWidth = ((ArrowType.Int) arrowType).getBitWidth();
        switch (bitWidth) {
          case 8:
            return Types.TINYINT;
          case 16:
            return Types.SMALLINT;
          case 32:
            return Types.INTEGER;
          case 64:
            return Types.BIGINT;
          default:
            break;
        }
        break;
      case Binary:
        return Types.VARBINARY;
      case FixedSizeBinary:
        return Types.BINARY;
      case LargeBinary:
        return Types.LONGVARBINARY;
      case Utf8:
        return Types.VARCHAR;
      case LargeUtf8:
        return Types.LONGVARCHAR;
      case Date:
        return Types.DATE;
      case Time:
        return Types.TIME;
      case Timestamp:
        return Types.TIMESTAMP;
      case Bool:
        return Types.BOOLEAN;
      case Decimal:
        return Types.DECIMAL;
      case FloatingPoint:
        final FloatingPointPrecision floatingPointPrecision =
            ((ArrowType.FloatingPoint) arrowType).getPrecision();
        switch (floatingPointPrecision) {
          case DOUBLE:
            return Types.DOUBLE;
          case SINGLE:
            return Types.FLOAT;
          default:
            break;
        }
        break;
      case List:
      case FixedSizeList:
      case LargeList:
        return Types.ARRAY;
      case Struct:
      case Duration:
      case Interval:
      case Map:
      case Union:
        return Types.JAVA_OBJECT;
      case NONE:
      case Null:
        return Types.NULL;
      default:
        break;
    }

    throw new IllegalArgumentException("Unsupported ArrowType " + arrowType);
  }
}

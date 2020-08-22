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

package org.apache.arrow.flight.sql;

import java.sql.Types;
import java.util.List;

import org.apache.arrow.flight.ActionType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * Utilities to work with Flight SQL semantics.
 */
public final class FlightSQLUtils {

  private static final int BIT_WIDTH8 = 8;
  private static final int BIT_WIDTH_16 = 16;
  private static final int BIT_WIDTH_32 = 32;
  private static final int BIT_WIDTH_64 = 64;
  private static final boolean IS_SIGNED_FALSE = false;
  private static final boolean IS_SIGNED_TRUE = true;

  public static final ActionType FLIGHT_SQL_GETSQLCAPABILITIES = new ActionType("GetSQLCapabilities",
          "Retrieves details of SQL capabilities of the Flight server. \n" +
                  "Request Message: N/A\n" +
                  "Response Message: SQLCapabilitiesResult");

  public static final ActionType FLIGHT_SQL_GETCATALOGS = new ActionType("GetCatalogs",
          "Retrieves a list of all catalogs available on the server. \n" +
                  "Request Message: GetCatalogsRequest\n" +
                  "Response Message: GetCatalogsResult");

  public static final ActionType FLIGHT_SQL_GETSCHEMAS = new ActionType("GetSchemas",
          "Retrieves a list of schemas available on the server. \n" +
                  "Request Message: GetSchemasRequest\n" +
                  "Response Message: GetSchemasResult");

  public static final ActionType FLIGHT_SQL_GETTABLES = new ActionType("GetTables",
          "Retrieves a list of tables available on the server. \n" +
                  "Request Message: GetTablesRequest\n" +
                  "Response Message: GetTablesResult");

  public static final ActionType FLIGHT_SQL_GETTABLETYPES = new ActionType("GetTableTypes",
          "Retrieves a list of table types available on the server. \n" +
                  "Request Message: N/A\n" +
                  "Response Message: GetTableTypesResult");

  public static final ActionType FLIGHT_SQL_GETPREPAREDSTATEMENT = new ActionType("GetPreparedStatement",
          "Creates a reusable prepared statement resource on the server. \n" +
                  "Request Message: ActionRequestGetPreparedStatement\n" +
                  "Response Message: ActionResponseGetPreparedStatement");

  public static final ActionType FLIGHT_SQL_CLOSEPREPAREDSTATEMENT = new ActionType("ClosePreparedStatement",
          "Closes a reusable prepared statement resource on the server. \n" +
                  "Request Message: ActionRequestClosePreparedStatement\n" +
                  "Response Message: N/A");

  public static final List<ActionType> FLIGHT_SQL_ACTIONS = ImmutableList.of(
          FLIGHT_SQL_GETSQLCAPABILITIES,
          FLIGHT_SQL_GETCATALOGS,
          FLIGHT_SQL_GETSCHEMAS,
          FLIGHT_SQL_GETTABLES,
          FLIGHT_SQL_GETTABLETYPES,
          FLIGHT_SQL_GETPREPAREDSTATEMENT,
          FLIGHT_SQL_CLOSEPREPAREDSTATEMENT
  );

  /**
   * Converts {@link java.sql.Types} values returned from JDBC Apis to Arrow types.
   *
   * @param jdbcDataType {@link java.sql.Types} value.
   * @param precision    Precision of the type.
   * @param scale        Scale of the type.
   * @return The Arrow equivalent type.
   */
  public static ArrowType getArrowTypeFromJDBCType(int jdbcDataType, int precision, int scale) {

    switch (jdbcDataType) {
      case Types.BIT:
      case Types.BOOLEAN:
        return ArrowType.Bool.INSTANCE;
      case Types.TINYINT:
        return new ArrowType.Int(BIT_WIDTH8, IS_SIGNED_TRUE);
      case Types.SMALLINT:
        return new ArrowType.Int(BIT_WIDTH_16, IS_SIGNED_TRUE);
      case Types.INTEGER:
        return new ArrowType.Int(BIT_WIDTH_32, IS_SIGNED_TRUE);
      case Types.BIGINT:
        return new ArrowType.Int(BIT_WIDTH_64, IS_SIGNED_TRUE);
      case Types.FLOAT:
      case Types.REAL:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case Types.DOUBLE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case Types.NUMERIC:
      case Types.DECIMAL:
        return new ArrowType.Decimal(precision, scale);
      case Types.DATE:
        return new ArrowType.Date(DateUnit.DAY);
      case Types.TIME:
        return new ArrowType.Time(TimeUnit.MILLISECOND, BIT_WIDTH_32);
      case Types.TIMESTAMP:
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return ArrowType.Binary.INSTANCE;
      case Types.NULL:
        return ArrowType.Null.INSTANCE;

      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.CLOB:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.NCLOB:

      case Types.OTHER:
      case Types.JAVA_OBJECT:
      case Types.DISTINCT:
      case Types.STRUCT:
      case Types.ARRAY:
      case Types.BLOB:
      case Types.REF:
      case Types.DATALINK:
      case Types.ROWID:
      case Types.SQLXML:
      case Types.REF_CURSOR:
      case Types.TIME_WITH_TIMEZONE:
      case Types.TIMESTAMP_WITH_TIMEZONE:
      default:
        return ArrowType.Utf8.INSTANCE;
      //        throw new UnsupportedOperationException();
    }
  }

  /**
   * Helper to parse {@link com.google.protobuf.Any} objects to the specific protobuf object.
   *
   * @param source the raw bytes source value.
   * @return the materialized protobuf object.
   */
  public static Any parseOrThrow(byte[] source) {
    try {
      return Any.parseFrom(source);
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError(e.getMessage());
    }
  }

  /**
   * Helper to unpack {@link com.google.protobuf.Any} objects to the specific protobuf object.
   *
   * @param source the parsed Source value.
   * @param as     the class to unpack as.
   * @param <T>    the class to unpack as.
   * @return the materialized protobuf object.
   */
  public static <T extends Message> T unpackOrThrow(Any source, Class<T> as) {
    try {
      return source.unpack(as);
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError(e.getMessage());
    }
  }

  /**
   * Helper to parse and unpack {@link com.google.protobuf.Any} objects to the specific protobuf object.
   *
   * @param source the raw bytes source value.
   * @param as     the class to unpack as.
   * @param <T>    the class to unpack as.
   * @return the materialized protobuf object.
   */
  public static <T extends Message> T unpackAndParseOrThrow(byte[] source, Class<T> as) {
    return unpackOrThrow(parseOrThrow(source), as);
  }
}

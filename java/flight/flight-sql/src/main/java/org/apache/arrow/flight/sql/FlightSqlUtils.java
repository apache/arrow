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

import java.util.List;

import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * Utilities to work with Flight SQL semantics.
 */
public final class FlightSqlUtils {
  public static final ActionType FLIGHT_SQL_CREATE_PREPARED_STATEMENT = new ActionType("CreatePreparedStatement",
      "Creates a reusable prepared statement resource on the server. \n" +
          "Request Message: ActionCreatePreparedStatementRequest\n" +
          "Response Message: ActionCreatePreparedStatementResult");

  public static final ActionType FLIGHT_SQL_CLOSE_PREPARED_STATEMENT = new ActionType("ClosePreparedStatement",
      "Closes a reusable prepared statement resource on the server. \n" +
          "Request Message: ActionClosePreparedStatementRequest\n" +
          "Response Message: N/A");

  public static final List<ActionType> FLIGHT_SQL_ACTIONS = ImmutableList.of(
      FLIGHT_SQL_CREATE_PREPARED_STATEMENT,
      FLIGHT_SQL_CLOSE_PREPARED_STATEMENT
  );

  /**
   * Helper to parse {@link com.google.protobuf.Any} objects to the specific protobuf object.
   *
   * @param source the raw bytes source value.
   * @return the materialized protobuf object.
   */
  public static Any parseOrThrow(byte[] source) {
    try {
      return Any.parseFrom(source);
    } catch (final InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("Received invalid message from remote.")
          .withCause(e)
          .toRuntimeException();
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
    } catch (final InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("Provided message cannot be unpacked as " + as.getName() + ": " + e)
          .withCause(e)
          .toRuntimeException();
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

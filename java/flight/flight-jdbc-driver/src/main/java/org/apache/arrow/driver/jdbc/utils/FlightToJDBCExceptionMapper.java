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

import java.sql.SQLException;
import java.util.Map;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;

import com.google.common.collect.ImmutableMap;

/**
 * Parent class for rpc exceptions.
 */
public class FlightToJDBCExceptionMapper {

  private static final String UNAUTHENTICATED = "28000";
  private static final String UNAUTHORIZED = "42000";
  private static final String UNAVAILABLE = "08001";
  private static final String UNIMPLEMENTED = "0A000";
  private static final String CANCELLED = "HY008";
  private static final String ALREADY_EXISTS = "21000";
  private static final String NOT_FOUND = "42000";
  private static final String TIMED_OUT = "HYT01";
  private static final String INVALID_ARGUMENT = "2200T";
  private static final String INTERNAL = "01000";
  private static final String UNKNOWN = "01000";

  private static final Map<FlightStatusCode, String> errorStatusSQLStateMap =
      new ImmutableMap.Builder<FlightStatusCode, String>()
          .put(FlightStatusCode.UNAUTHENTICATED, UNAUTHENTICATED)
          .put(FlightStatusCode.UNAUTHORIZED, UNAUTHORIZED)
          .put(FlightStatusCode.UNAVAILABLE, UNAVAILABLE)
          .put(FlightStatusCode.UNIMPLEMENTED, UNIMPLEMENTED)
          .put(FlightStatusCode.CANCELLED, CANCELLED)
          .put(FlightStatusCode.ALREADY_EXISTS, ALREADY_EXISTS)
          .put(FlightStatusCode.NOT_FOUND, NOT_FOUND)
          .put(FlightStatusCode.TIMED_OUT, TIMED_OUT)
          .put(FlightStatusCode.INVALID_ARGUMENT, INVALID_ARGUMENT)
          .put(FlightStatusCode.INTERNAL, INTERNAL)
          .put(FlightStatusCode.UNKNOWN, UNKNOWN).build();

  private FlightToJDBCExceptionMapper() {}

  public static SQLException map(FlightRuntimeException flightRuntimeException) {
    return map(flightRuntimeException, flightRuntimeException.getMessage());
  }

  /**
   * Map the given RpcException into an equivalent SQLException.
   * <p>
   * An appropriate SQLState will be chosen for the RpcException, if one is available.
   *
   * @param flightRuntimeException The remote exception to map.
   * @param message                The message format string to use for the SQLException.
   * @return The equivalently mapped SQLException.
   */
  public static SQLException map(FlightRuntimeException flightRuntimeException, String message) {
    return new SQLException(message, getSqlCodeFromRpcExceptionType(flightRuntimeException), flightRuntimeException);
  }

  private static String getSqlCodeFromRpcExceptionType(FlightRuntimeException flightRuntimeException) {
    return errorStatusSQLStateMap.get(flightRuntimeException.status().code());
  }
}

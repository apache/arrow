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
public class RpcExceptionMapper {

  /**
   * Status code for when authentication fails.
   */
  public static final String AUTH_FAILED = "AUTH_FAILED";
  /**
   * Status code for when attempting to use a connection that is no longer valid.
   */
  public static final String CONNECTION_INVALID = "CONNECTION_INVALID";

  private static final String UNAUTHENTICATED = "28000";
  private static final String DISCONNECT_ERROR = "01002";
  private static final String UNAVAILABLE = "08001";
  private static final String STATUS_PREFIX = " Status: ";

  private static final Map<FlightStatusCode, String> errorStatusSQLStateMap =
      new ImmutableMap.Builder<FlightStatusCode, String>().put(FlightStatusCode.UNAUTHENTICATED, UNAUTHENTICATED)
          .build();

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
   * @param args                   The arguments for the message.
   * @return The equivalently mapped SQLException.
   */
  public static SQLException map(FlightRuntimeException flightRuntimeException, String message, String... args) {
    if (message != null && args != null) {
      message = String.format(message, (Object) args);
    }
    return new SQLException(message, getSqlCodeFromRpcExceptionType(flightRuntimeException), flightRuntimeException);
  }

  private static String getSqlCodeFromRpcExceptionType(FlightRuntimeException flightRuntimeException) {
    if (flightRuntimeException.status().code() == FlightStatusCode.UNAUTHENTICATED) {
      return UNAUTHENTICATED;
    } else {
      return null;
    }
  }
}
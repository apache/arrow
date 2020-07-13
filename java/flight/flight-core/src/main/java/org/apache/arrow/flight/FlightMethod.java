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

package org.apache.arrow.flight;

import org.apache.arrow.flight.impl.FlightServiceGrpc;

/**
 * All the RPC methods available in Flight.
 */
public enum FlightMethod {
  HANDSHAKE,
  LIST_FLIGHTS,
  GET_FLIGHT_INFO,
  GET_SCHEMA,
  DO_GET,
  DO_PUT,
  DO_ACTION,
  LIST_ACTIONS,
  DO_EXCHANGE,
  ;

  /**
   * Convert a method name string into a {@link FlightMethod}.
   *
   * @throws IllegalArgumentException if the method name is not valid.
   */
  public static FlightMethod fromProtocol(final String methodName) {
    if (FlightServiceGrpc.getHandshakeMethod().getFullMethodName().equals(methodName)) {
      return HANDSHAKE;
    } else if (FlightServiceGrpc.getListFlightsMethod().getFullMethodName().equals(methodName)) {
      return LIST_FLIGHTS;
    } else if (FlightServiceGrpc.getGetFlightInfoMethod().getFullMethodName().equals(methodName)) {
      return GET_FLIGHT_INFO;
    } else if (FlightServiceGrpc.getGetSchemaMethod().getFullMethodName().equals(methodName)) {
      return GET_SCHEMA;
    } else if (FlightServiceGrpc.getDoGetMethod().getFullMethodName().equals(methodName)) {
      return DO_GET;
    } else if (FlightServiceGrpc.getDoPutMethod().getFullMethodName().equals(methodName)) {
      return DO_PUT;
    } else if (FlightServiceGrpc.getDoActionMethod().getFullMethodName().equals(methodName)) {
      return DO_ACTION;
    } else if (FlightServiceGrpc.getListActionsMethod().getFullMethodName().equals(methodName)) {
      return LIST_ACTIONS;
    } else if (FlightServiceGrpc.getDoExchangeMethod().getFullMethodName().equals(methodName)) {
      return DO_EXCHANGE;
    }
    throw new IllegalArgumentException("Not a Flight method name in gRPC: " + methodName);
  }
}

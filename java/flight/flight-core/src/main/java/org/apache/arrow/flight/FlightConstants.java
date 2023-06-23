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

/**
 * String constants relevant to flight implementations.
 */
public interface FlightConstants {

  String SERVICE = "arrow.flight.protocol.FlightService";

  FlightServerMiddleware.Key<ServerHeaderMiddleware> HEADER_KEY =
      FlightServerMiddleware.Key.of("org.apache.arrow.flight.ServerHeaderMiddleware");

  ActionType CANCEL_FLIGHT_INFO = new ActionType("CancelFlightInfo",
      "Explicitly cancel a running FlightInfo.\n" +
          "Request Message: FlightInfo to be canceled\n" +
          "Response Message: ActionCancelFlightInfoResult");

  ActionType CLOSE_FLIGHT_INFO = new ActionType("CloseFlightInfo",
      "Close the given FlightInfo explicitly.\n" +
          "Request Message: FlightInfo to be closed\n" +
          "Response Message: N/A");
  ActionType RENEW_FLIGHT_ENDPOINT = new ActionType("RenewFlightEndpoint",
      "Extend expiration time of the given FlightEndpoint.\n" +
          "Request Message: FlightEndpoint to be renewed\n" +
          "Response Message: Renewed FlightEndpoint");
}

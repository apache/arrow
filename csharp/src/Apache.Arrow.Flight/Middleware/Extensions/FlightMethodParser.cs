// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Apache.Arrow.Flight.Middleware.Models;

namespace Apache.Arrow.Flight.Middleware.Extensions;

public static class FlightMethodParser
{
    /// <summary>
    /// Parses the gRPC full method name (e.g., "/arrow.flight.protocol.FlightService/DoGet")
    /// and maps it to a known FlightMethod.
    /// </summary>
    /// <param name="fullMethodName">gRPC method name</param>
    /// <returns>Parsed FlightMethod</returns>
    public static FlightMethod Parse(string fullMethodName)
    {
        if (string.IsNullOrWhiteSpace(fullMethodName))
            return FlightMethod.Unknown;

        var parts = fullMethodName.Split('/');
        if (parts.Length < 2)
            return FlightMethod.Unknown;

        var methodName = parts[parts.Length - 1];

        return methodName switch
        {
            "Handshake" => FlightMethod.Handshake,
            "ListFlights" => FlightMethod.ListFlights,
            "GetFlightInfo" => FlightMethod.GetFlightInfo,
            "GetSchema" => FlightMethod.GetSchema,
            "DoGet" => FlightMethod.DoGet,
            "DoPut" => FlightMethod.DoPut,
            "DoExchange" => FlightMethod.DoExchange,
            "DoAction" => FlightMethod.DoAction,
            "ListActions" => FlightMethod.ListActions,
            "CancelFlightInfo" => FlightMethod.CancelFlightInfo,
            _ => FlightMethod.Unknown
        };
    }

    public static string ParseMethodName(string fullMethodName)
    {
        if (string.IsNullOrWhiteSpace(fullMethodName))
            return "Unknown";

        var parts = fullMethodName.Split('/');
        return parts.Length >= 2 ? parts[parts.Length - 1] : "Unknown";
    }
}
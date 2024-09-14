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

#if NET46_OR_GREATER

using Apache.Arrow.Flight.Protocol;
using Apache.Arrow.Flight.Server.Internal;
using Grpc.Core;

namespace Apache.Arrow.Flight.Server
{
    public static class GrpcCoreFlightServerExtensions
    {
        /// <summary>
        /// Create a ServerServiceDefinition for use with a <see href="https://grpc.github.io/grpc/csharp/api/Grpc.Core.Server.html">Grpc.Core Server</see>
        //  This allows running a flight server on pre-Kestrel .net Framework versions
        /// </summary>
        /// <param name="flightServer"></param>
        /// <returns></returns>
        public static ServerServiceDefinition CreateServiceDefinition(this FlightServer flightServer)
        {
            return FlightService.BindService(new FlightServerImplementation(flightServer));
        }
    }
}

#endif

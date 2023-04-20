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

using System;
using System.Collections.Generic;
using System.Text;
using Apache.Arrow.Flight.Internal;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Flight
{
    public class FlightInfo
    {
        internal FlightInfo(Protocol.FlightInfo flightInfo)
        {
            Schema = FlightMessageSerializer.DecodeSchema(flightInfo.Schema.Memory);
            Descriptor = new FlightDescriptor(flightInfo.FlightDescriptor);

            var endpoints = new List<FlightEndpoint>();
            foreach(var endpoint in flightInfo.Endpoint)
            {
                endpoints.Add(new FlightEndpoint(endpoint));
            }
            Endpoints = endpoints;

            TotalBytes = flightInfo.TotalBytes;
            TotalRecords = flightInfo.TotalRecords;
        }

        public FlightInfo(Schema schema, FlightDescriptor descriptor, IReadOnlyList<FlightEndpoint> endpoints, long totalRecords = -1, long totalBytes = -1)
        {
            Schema = schema;
            Descriptor = descriptor;
            Endpoints = endpoints;
            TotalBytes = totalBytes;
            TotalRecords = totalRecords;
        }

        public FlightDescriptor Descriptor { get; }

        public Schema Schema { get; }

        public long TotalBytes { get; }

        public long TotalRecords { get; }

        public IReadOnlyList<FlightEndpoint> Endpoints { get; }

        internal Protocol.FlightInfo ToProtocol()
        {
            var serializedSchema = SchemaWriter.SerializeSchema(Schema);
            var response = new Protocol.FlightInfo()
            {
                Schema = serializedSchema,
                FlightDescriptor = Descriptor.ToProtocol(),
                TotalBytes = TotalBytes,
                TotalRecords = TotalRecords
            };

            foreach(var endpoint in Endpoints)
            {
                response.Endpoint.Add(endpoint.ToProtocol());
            }

            return response;
        }
    }
}

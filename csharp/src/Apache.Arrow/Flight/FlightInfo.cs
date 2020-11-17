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
using Apache.Arrow.Flight.Writer;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Flight
{
    public class FlightInfo
    {
        private readonly Schema _schema;
        private readonly FlightDescriptor _flightDescriptor;
        private readonly IList<FlightEndpoint> _flightEndpoints;
        
        public FlightInfo(Protocol.FlightInfo flightInfo)
        {
            _schema = FlightMessageSerializer.DecodeSchema(flightInfo.Schema.Memory);
            _flightDescriptor = new FlightDescriptor(flightInfo.FlightDescriptor);

            var endpoints = new List<FlightEndpoint>();
            foreach(var endpoint in flightInfo.Endpoint)
            {
                endpoints.Add(new FlightEndpoint(endpoint));
            }
            _flightEndpoints = endpoints;
        }

        public FlightInfo(Schema schema, FlightDescriptor flightDescriptor, IList<FlightEndpoint> flightEndpoints)
        {
            _schema = schema;
            _flightDescriptor = flightDescriptor;
            _flightEndpoints = flightEndpoints;
        }

        public IEnumerable<FlightEndpoint> Endpoints => _flightEndpoints;

        public Protocol.FlightInfo ToProtocol()
        {
            var serializedSchema = SchemaWriter.SerializeSchema(_schema);
            var response = new Protocol.FlightInfo()
            {
                Schema = serializedSchema,
                FlightDescriptor = _flightDescriptor.ToProtocol()
            };

            foreach(var endpoint in _flightEndpoints)
            {
                response.Endpoint.Add(endpoint.ToProtocol());
            }

            return response;
        }
    }
}

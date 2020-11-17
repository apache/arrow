﻿// Licensed to the Apache Software Foundation (ASF) under one or more
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
using System.Linq;
using System.Text;

namespace Apache.Arrow.Flight
{
    public class FlightEndpoint
    {
        private readonly Ticket _ticket;
        private readonly IReadOnlyList<Location> _locations;
        internal FlightEndpoint(Protocol.FlightEndpoint flightEndpoint)
        {
            _ticket = new Ticket(flightEndpoint.Ticket);
            _locations = flightEndpoint.Location.Select(x => new Location(x)).ToList();
        }

        public FlightEndpoint(Ticket ticket, IReadOnlyList<Location> locations)
        {
            _ticket = ticket;
            _locations = locations;
        }

        public Ticket Ticket => _ticket;

        public IEnumerable<Location> Locations => _locations;

        public Protocol.FlightEndpoint ToProtocol()
        {
            var output = new Protocol.FlightEndpoint()
            {
                Ticket = _ticket.ToProtocol()
            };

            foreach(var location in _locations)
            {
                output.Location.Add(location.ToProtocol());
            }
            return output;
        }
    }
}

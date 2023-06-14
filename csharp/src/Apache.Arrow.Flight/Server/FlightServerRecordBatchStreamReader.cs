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

using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Apache.Arrow.Flight.Server
{
    public class FlightServerRecordBatchStreamReader : FlightRecordBatchStreamReader
    {
        public FlightServerRecordBatchStreamReader(IAsyncStreamReader<FlightData> flightDataStream) : base(new ProtocolFlightDataStreamReaderProxy(flightDataStream))
        {
        }

        internal FlightServerRecordBatchStreamReader(IAsyncStreamReader<Protocol.FlightData> flightDataStream) : base(flightDataStream)
        {
        }

        public ValueTask<FlightDescriptor> FlightDescriptor => GetFlightDescriptor();

        private class ProtocolFlightDataStreamReaderProxy : IAsyncStreamReader<Protocol.FlightData>
        {
             private readonly IAsyncStreamReader<FlightData> _clientStreamReader;

            internal ProtocolFlightDataStreamReaderProxy(IAsyncStreamReader<FlightData> clientStreamReader)
            {
                _clientStreamReader = clientStreamReader;
            }

            public Task<bool> MoveNext(CancellationToken cancellationToken) => _clientStreamReader.MoveNext(cancellationToken);

            public Protocol.FlightData Current => _clientStreamReader.Current.ToProtocol();
        }
    }
}

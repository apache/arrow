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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flatbuf;
using Apache.Arrow.Flight.Protocol;
using Apache.Arrow.Ipc;
using Grpc.Core;

namespace Apache.Arrow.Flight
{
    /// <summary>
    /// Stream of record batches
    ///
    /// Use MoveNext() and Current to iterate over the batches.
    /// There are also gRPC helper functions such as ToListAsync() etc.
    /// </summary>
    public class RecordBatchStreamReader : IAsyncStreamReader<RecordBatch>
    {
        private readonly RecordBatcReaderImplementation _arrowReaderImplementation;

        public RecordBatchStreamReader(IAsyncStreamReader<Protocol.FlightData> flightDataStream)
        {
            _arrowReaderImplementation = new RecordBatcReaderImplementation(flightDataStream);
        }

        public ValueTask<Schema> Schema => _arrowReaderImplementation.ReadSchema();

        public ValueTask<FlightDescriptor> FlightDescriptor => _arrowReaderImplementation.ReadFlightDescriptor();

        public RecordBatch Current { get; private set; }

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            Current = await _arrowReaderImplementation.ReadNextRecordBatchAsync(cancellationToken);

            return Current != null;
        }
    }
}

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
using Apache.Arrow.Flight.Internal;
using Apache.Arrow.Flight.Protocol;
using Apache.Arrow.Ipc;
using Google.Protobuf;
using Grpc.Core;

namespace Apache.Arrow.Flight
{
    /// <summary>
    /// Stream of record batches
    ///
    /// Use MoveNext() and Current to iterate over the batches.
    /// There are also gRPC helper functions such as ToListAsync() etc.
    /// </summary>
    public abstract class FlightRecordBatchStreamReader : IAsyncStreamReader<RecordBatch>, IAsyncEnumerable<RecordBatch>, IDisposable
    {
        //Temporary until .NET 5.0 upgrade
        private static ValueTask CompletedValueTask = new ValueTask();

        private readonly RecordBatchReaderImplementation _arrowReaderImplementation;

        private protected FlightRecordBatchStreamReader(IAsyncStreamReader<Protocol.FlightData> flightDataStream)
        {
            _arrowReaderImplementation = new RecordBatchReaderImplementation(flightDataStream);
        }

        public ValueTask<Schema> Schema => _arrowReaderImplementation.ReadSchema();

        internal ValueTask<FlightDescriptor> GetFlightDescriptor()
        {
            return _arrowReaderImplementation.ReadFlightDescriptor();
        }        

        /// <summary>
        /// Get the application metadata from the latest received record batch
        /// </summary>
        public IReadOnlyList<ByteString> ApplicationMetadata => _arrowReaderImplementation.ApplicationMetadata;

        public RecordBatch Current { get; private set; }

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            Current = await _arrowReaderImplementation.ReadNextRecordBatchAsync(cancellationToken);

            return Current != null;
        }

        public IAsyncEnumerator<RecordBatch> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new AsyncEnumerator(this, cancellationToken);
        }

        public void Dispose()
        {
            _arrowReaderImplementation.Dispose();
        }

        private class AsyncEnumerator : IAsyncEnumerator<RecordBatch>
        {
            private readonly FlightRecordBatchStreamReader _flightRecordBatchStreamReader;
            private readonly CancellationToken _cancellationToken;

            internal AsyncEnumerator(FlightRecordBatchStreamReader flightRecordBatchStreamReader, CancellationToken cancellationToken)
            {
                _flightRecordBatchStreamReader = flightRecordBatchStreamReader;
                _cancellationToken = cancellationToken;
            }

            public RecordBatch Current => _flightRecordBatchStreamReader.Current;

            public async ValueTask<bool> MoveNextAsync()
            {
                return await _flightRecordBatchStreamReader.MoveNext(_cancellationToken);
            }

            public ValueTask DisposeAsync()
            {
                _flightRecordBatchStreamReader.Dispose();
                return CompletedValueTask;
            }
        }
    }
}

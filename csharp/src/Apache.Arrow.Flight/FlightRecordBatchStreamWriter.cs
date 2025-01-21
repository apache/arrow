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
using System.Threading.Tasks;
using Apache.Arrow.Flight.Internal;
using Apache.Arrow.Flight.Protocol;
using Google.Protobuf;
using Grpc.Core;

namespace Apache.Arrow.Flight
{
    public abstract class FlightRecordBatchStreamWriter : IAsyncStreamWriter<RecordBatch>, IDisposable
    {
        private FlightDataStream _flightDataStream;
        private readonly IAsyncStreamWriter<Protocol.FlightData> _clientStreamWriter;
        private readonly FlightDescriptor _flightDescriptor;

        private bool _disposed;

        private protected FlightRecordBatchStreamWriter(IAsyncStreamWriter<Protocol.FlightData> clientStreamWriter, FlightDescriptor flightDescriptor)
        {
            _clientStreamWriter = clientStreamWriter;
            _flightDescriptor = flightDescriptor;
        }

        /// <summary>
        /// Configure the data stream to write to.
        /// </summary>
        /// <remarks>
        /// The stream will be set up automatically when writing a RecordBatch if required,
        /// but calling this method before writing any data allows handling empty streams.
        /// </remarks>
        /// <param name="schema">The schema of data to be written to this stream</param>
        public async Task SetupStream(Schema schema)
        {
            if (_flightDataStream != null)
            {
                throw new InvalidOperationException("Flight data stream is already set");
            }
            _flightDataStream = new FlightDataStream(_clientStreamWriter, _flightDescriptor, schema);
            await _flightDataStream.SendSchema().ConfigureAwait(false);
        }

        public WriteOptions WriteOptions { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public Task WriteAsync(RecordBatch message)
        {
            return WriteAsync(message, default);
        }

        public async Task WriteAsync(RecordBatch message, ByteString applicationMetadata)
        {
            if (_flightDataStream == null)
            {
                await SetupStream(message.Schema).ConfigureAwait(false);
            }

            await _flightDataStream.Write(message, applicationMetadata);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _flightDataStream?.Dispose();
                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
    }
}

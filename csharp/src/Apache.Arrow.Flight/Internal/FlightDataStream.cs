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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flatbuf;
using Apache.Arrow.Ipc;
using Google.FlatBuffers;
using Google.Protobuf;
using Grpc.Core;

namespace Apache.Arrow.Flight.Internal
{
    /// <summary>
    /// Handles writing record batches as flight data
    /// </summary>
    internal class FlightDataStream : ArrowStreamWriter
    {
        private readonly FlightDescriptor _flightDescriptor;
        private readonly IAsyncStreamWriter<Protocol.FlightData> _clientStreamWriter;
        private Protocol.FlightData _currentFlightData;
        private ByteString _currentAppMetadata;

        public FlightDataStream(IAsyncStreamWriter<Protocol.FlightData> clientStreamWriter, FlightDescriptor flightDescriptor, Schema schema)
            : base(new MemoryStream(), schema)
        {
            _clientStreamWriter = clientStreamWriter;
            _flightDescriptor = flightDescriptor;
        }

        private async Task SendSchema()
        {
            _currentFlightData = new Protocol.FlightData();

            if(_flightDescriptor != null)
            {
                _currentFlightData.FlightDescriptor = _flightDescriptor.ToProtocol();
            }

            var offset = SerializeSchema(Schema);
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            await WriteMessageAsync(MessageHeader.Schema, offset, 0, cancellationTokenSource.Token).ConfigureAwait(false);
            await _clientStreamWriter.WriteAsync(_currentFlightData).ConfigureAwait(false);
            HasWrittenSchema = true;
        }

        private void ResetStream()
        {
            this.BaseStream.Position = 0;
            this.BaseStream.SetLength(0);
        }

        private void ResetFlightData()
        {
            _currentFlightData = new Protocol.FlightData();
        }

        private void AddMetadata()
        {
            if (_currentAppMetadata != null)
            {
                _currentFlightData.AppMetadata = _currentAppMetadata;
            }
        }

        private async Task SetFlightDataBodyFromBaseStreamAsync(CancellationToken cancellationToken)
        {
            BaseStream.Position = 0;
            var body = await ByteString.FromStreamAsync(BaseStream, cancellationToken).ConfigureAwait(false);
            _currentFlightData.DataBody = body;
        }

        private async Task WriteFlightDataAsync()
        {
            await _clientStreamWriter.WriteAsync(_currentFlightData).ConfigureAwait(false);
        }

        public async Task Write(RecordBatch recordBatch, ByteString applicationMetadata)
        {
            _currentAppMetadata = applicationMetadata;
            if (!HasWrittenSchema)
            {
                await SendSchema().ConfigureAwait(false);
            }
            ResetStream();
            ResetFlightData();

            await WriteRecordBatchAsync(recordBatch).ConfigureAwait(false);
        }

        public override async Task WriteRecordBatchAsync(RecordBatch recordBatch, CancellationToken cancellationToken = default)
        {
            await WriteRecordBatchInternalAsync(recordBatch, cancellationToken);

            // Consume the MemoryStream and write to the flight stream
            await SetFlightDataBodyFromBaseStreamAsync(cancellationToken).ConfigureAwait(false);
            AddMetadata();
            await WriteFlightDataAsync().ConfigureAwait(false);

            HasWrittenDictionaryBatch = false; // force the dictionary to be sent again with the next batch
        }

        private protected override async Task WriteDictionariesAsync(DictionaryMemo dictionaryMemo, CancellationToken cancellationToken)
        {
            await base.WriteDictionariesAsync(dictionaryMemo, cancellationToken).ConfigureAwait(false);

            // Consume the MemoryStream and write to the flight stream
            await SetFlightDataBodyFromBaseStreamAsync(cancellationToken).ConfigureAwait(false);
            await WriteFlightDataAsync().ConfigureAwait(false);
            // Reset the stream for the next dictionary or record batch
            ResetStream();
            ResetFlightData();
        }

        private protected override ValueTask<long> WriteMessageAsync<T>(MessageHeader headerType, Offset<T> headerOffset, int bodyLength, CancellationToken cancellationToken)
        {
            Offset<Flatbuf.Message> messageOffset = Flatbuf.Message.CreateMessage(
                Builder, CurrentMetadataVersion, headerType, headerOffset.Value,
                bodyLength);

            Builder.Finish(messageOffset.Value);

            ReadOnlyMemory<byte> messageData = Builder.DataBuffer.ToReadOnlyMemory(Builder.DataBuffer.Position, Builder.Offset);

            _currentFlightData.DataHeader = ByteString.CopyFrom(messageData.Span);

            return new ValueTask<long>(0);
        }
    }
}

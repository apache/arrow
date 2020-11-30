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
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flatbuf;
using Apache.Arrow.Flight.Protocol;
using Apache.Arrow.Ipc;
using FlatBuffers;
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
        private readonly IAsyncStreamWriter<FlightData> _clientStreamWriter;
        private Protocol.FlightData _currentFlightData;

        public FlightDataStream(IAsyncStreamWriter<FlightData> clientStreamWriter, FlightDescriptor flightDescriptor, Schema schema)
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

        public async Task Write(RecordBatch recordBatch, ByteString applicationMetadata)
        {
            if (!HasWrittenSchema)
            {
                await SendSchema().ConfigureAwait(false);
            }
            ResetStream();

            _currentFlightData = new Protocol.FlightData();

            if(applicationMetadata != null)
            {
                _currentFlightData.AppMetadata = applicationMetadata;
            }

            await WriteRecordBatchInternalAsync(recordBatch).ConfigureAwait(false);

            //Reset stream position
            this.BaseStream.Position = 0;
            var bodyData = await ByteString.FromStreamAsync(this.BaseStream).ConfigureAwait(false);

            _currentFlightData.DataBody = bodyData;
            await _clientStreamWriter.WriteAsync(_currentFlightData).ConfigureAwait(false);
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

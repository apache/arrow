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
using Apache.Arrow.Ipc;
using Google.Protobuf;
using Grpc.Core;

namespace Apache.Arrow.Flight.Internal
{
    internal class RecordBatchReaderImplementation : ArrowReaderImplementation
    {
        private readonly IAsyncStreamReader<Protocol.FlightData> _flightDataStream;
        private FlightDescriptor _flightDescriptor;
        private readonly List<ByteString> _applicationMetadatas;

        public RecordBatchReaderImplementation(IAsyncStreamReader<Protocol.FlightData> streamReader)
        {
            _flightDataStream = streamReader;
            _applicationMetadatas = new List<ByteString>();
        }

        public override RecordBatch ReadNextRecordBatch()
        {
            throw new NotImplementedException();
        }

        public IReadOnlyList<ByteString> ApplicationMetadata => _applicationMetadatas;

        public async ValueTask<FlightDescriptor> ReadFlightDescriptor()
        {
            if (!HasReadSchema)
            {
                await ReadSchemaAsync(CancellationToken.None).ConfigureAwait(false);
            }
            return _flightDescriptor;
        }

        public async ValueTask<Schema> GetSchemaAsync()
        {
            if (!HasReadSchema)
            {
                await ReadSchemaAsync(CancellationToken.None).ConfigureAwait(false);
            }
            return _schema;
        }

        public override void ReadSchema()
        {
            ReadSchemaAsync(CancellationToken.None).AsTask().Wait();
        }

        public override async ValueTask ReadSchemaAsync(CancellationToken cancellationToken)
        {
            while (!HasReadSchema)
            {
                var moveNextResult = await _flightDataStream.MoveNext(cancellationToken).ConfigureAwait(false);
                if (!moveNextResult)
                {
                    throw new Exception("No records or schema in this flight");
                }

                if (_flightDescriptor == null && _flightDataStream.Current.FlightDescriptor != null)
                {
                    _flightDescriptor = new FlightDescriptor(_flightDataStream.Current.FlightDescriptor);
                }

                // AppMetadata will never be null, but length 0 if empty
                // Those are skipped
                if(_flightDataStream.Current.AppMetadata.Length > 0)
                {
                    _applicationMetadatas.Add(_flightDataStream.Current.AppMetadata);
                }

                var header = _flightDataStream.Current.DataHeader.Memory;
                if (header.IsEmpty)
                {
                    // Clients may send a first message with a descriptor only and no schema
                    continue;
                }

                Message message = Message.GetRootAsMessage(ArrowReaderImplementation.CreateByteBuffer(header));

                switch (message.HeaderType)
                {
                    case MessageHeader.Schema:
                        _schema = FlightMessageSerializer.DecodeSchema(message.ByteBuffer);
                        break;
                    default:
                        throw new Exception($"Expected schema as the first message, but got: {message.HeaderType.ToString()}");
                }
            }
        }

        public override async ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken)
        {
            _applicationMetadatas.Clear(); //Clear any metadata from previous calls

            if (!HasReadSchema)
            {
                await ReadSchemaAsync(cancellationToken).ConfigureAwait(false);
            }
            var moveNextResult = await _flightDataStream.MoveNext().ConfigureAwait(false);
            if (moveNextResult)
            {
                //AppMetadata will never be null, but length 0 if empty
                //Those are skipped
                if (_flightDataStream.Current.AppMetadata.Length > 0)
                {
                    _applicationMetadatas.Add(_flightDataStream.Current.AppMetadata);
                }

                var header = _flightDataStream.Current.DataHeader.Memory;
                Message message = Message.GetRootAsMessage(CreateByteBuffer(header));

                switch (message.HeaderType)
                {
                    case MessageHeader.RecordBatch:
                        var body = _flightDataStream.Current.DataBody.Memory;
                        return CreateArrowObjectFromMessage(message, CreateByteBuffer(body.Slice(0, (int)message.BodyLength)), null);
                    default:
                        throw new NotImplementedException();
                }
            }
            return null;
        }
    }
}

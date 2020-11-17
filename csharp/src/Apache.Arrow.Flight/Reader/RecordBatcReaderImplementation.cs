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
using Grpc.Core;

namespace Apache.Arrow.Flight
{
    internal class RecordBatcReaderImplementation : ArrowReaderImplementation
    {
        private readonly IAsyncStreamReader<Protocol.FlightData> _flightDataStream;
        private FlightDescriptor _flightDescriptor;

        public RecordBatcReaderImplementation(IAsyncStreamReader<Protocol.FlightData> streamReader)
        {
            _flightDataStream = streamReader;
        }

        public override RecordBatch ReadNextRecordBatch()
        {
            throw new NotImplementedException();
        }

        public async ValueTask<FlightDescriptor> ReadFlightDescriptor()
        {
            if (!HasReadSchema)
            {
                await ReadSchema();
            }
            return _flightDescriptor;
        }

        public async ValueTask<Schema> ReadSchema()
        {
            if (HasReadSchema)
            {
                return Schema;
            }

            var moveNextResult = await _flightDataStream.MoveNext();

            if (!moveNextResult)
            {
                throw new Exception("No records or schema in this flight");
            }

            var header = _flightDataStream.Current.DataHeader.Memory;
            Message message = Message.GetRootAsMessage(
                ArrowReaderImplementation.CreateByteBuffer(header));


            if(_flightDataStream.Current.FlightDescriptor != null)
            {
                _flightDescriptor = new FlightDescriptor(_flightDataStream.Current.FlightDescriptor);
            }

            switch (message.HeaderType)
            {
                case MessageHeader.Schema:
                    Schema = FlightMessageSerializer.DecodeSchema(message.ByteBuffer);
                    break;
                default:
                    throw new Exception($"Expected schema as the first message, but got: {message.HeaderType.ToString()}");
            }
            return Schema;
        }

        public override async ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken)
        {
            if (!HasReadSchema)
            {
                await ReadSchema();
            }
            var moveNextResult = await _flightDataStream.MoveNext();
            if (moveNextResult)
            {
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

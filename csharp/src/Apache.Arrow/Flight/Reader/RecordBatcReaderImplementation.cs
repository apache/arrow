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

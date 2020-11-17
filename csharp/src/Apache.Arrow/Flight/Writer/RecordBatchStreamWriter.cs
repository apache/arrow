using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Protocol;
using Grpc.Core;

namespace Apache.Arrow.Flight
{
    public class RecordBatchStreamWriter : IAsyncStreamWriter<RecordBatch>, IDisposable
    {
        private FlightDataStream _flightDataStream;
        private readonly IAsyncStreamWriter<FlightData> _clientStreamWriter;
        private readonly FlightDescriptor _flightDescriptor;

        private bool _disposed;

        public RecordBatchStreamWriter(IAsyncStreamWriter<FlightData> clientStreamWriter, FlightDescriptor flightDescriptor)
        {
            _clientStreamWriter = clientStreamWriter;
            _flightDescriptor = flightDescriptor;
        }

        private void SetupStream(Schema schema)
        {
            _flightDataStream = new FlightDataStream(_clientStreamWriter, _flightDescriptor, schema);
        }

        public WriteOptions WriteOptions { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public Task WriteAsync(RecordBatch message)
        {
            if (_flightDataStream == null)
            {
                SetupStream(message.Schema);
            }

            return _flightDataStream.Write(message);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _flightDataStream.Dispose();
                _disposed = true;
            }
        }

        ~RecordBatchStreamWriter()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
        }
    }
}

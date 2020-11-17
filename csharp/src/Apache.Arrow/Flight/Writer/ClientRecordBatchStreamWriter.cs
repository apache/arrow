using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Protocol;
using Grpc.Core;

namespace Apache.Arrow.Flight.Writer
{
    public class ClientRecordBatchStreamWriter : RecordBatchStreamWriter, IClientStreamWriter<RecordBatch>
    {
        private readonly IClientStreamWriter<FlightData> _clientStreamWriter;
        private bool _completed = false;
        public ClientRecordBatchStreamWriter(IClientStreamWriter<FlightData> clientStreamWriter, FlightDescriptor flightDescriptor) : base(clientStreamWriter, flightDescriptor)
        {
            _clientStreamWriter = clientStreamWriter;
        }

        protected override void Dispose(bool disposing)
        {
            CompleteAsync().Wait();
            base.Dispose(disposing);
        }

        public async Task CompleteAsync()
        {
            if (_completed)
            {
                return;
            }

            await _clientStreamWriter.CompleteAsync();
            _completed = true;
        }
    }
}

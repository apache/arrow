using System;
using System.Collections.Generic;
using System.Text;
using Apache.Arrow.Flight.Protocol;
using Grpc.Core;

namespace Apache.Arrow.Flight.Writer
{
    public class ServerRecordBatchStreamWriter : RecordBatchStreamWriter, IServerStreamWriter<RecordBatch>
    {
        public ServerRecordBatchStreamWriter(IServerStreamWriter<FlightData> clientStreamWriter) : base(clientStreamWriter, null)
        {
        }
    }
}

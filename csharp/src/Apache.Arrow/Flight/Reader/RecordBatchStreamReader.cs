using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flatbuf;
using Apache.Arrow.Flight.Protocol;
using Apache.Arrow.Ipc;
using Grpc.Core;

namespace Apache.Arrow.Flight
{
    /// <summary>
    /// Stream of record batches
    ///
    /// Use MoveNext() and Current to iterate over the batches.
    /// There are also gRPC helper functions such as ToListAsync() etc.
    /// </summary>
    public class RecordBatchStreamReader : IAsyncStreamReader<RecordBatch>
    {
        private readonly RecordBatcReaderImplementation _arrowReaderImplementation;

        public RecordBatchStreamReader(IAsyncStreamReader<Protocol.FlightData> flightDataStream)
        {
            _arrowReaderImplementation = new RecordBatcReaderImplementation(flightDataStream);
        }

        public ValueTask<Schema> Schema => _arrowReaderImplementation.ReadSchema();

        public ValueTask<FlightDescriptor> FlightDescriptor => _arrowReaderImplementation.ReadFlightDescriptor();

        public RecordBatch Current { get; private set; }

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            Current = await _arrowReaderImplementation.ReadNextRecordBatchAsync(cancellationToken);

            return Current != null;
        }
    }
}

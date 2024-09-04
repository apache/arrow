using System.Collections.Generic;
using System.Linq;

namespace Apache.Arrow.Flight.Sql.TestWeb;

public class FlightSqlHolder
{
    private readonly FlightDescriptor _flightDescriptor;
    private readonly Schema _schema;
    private readonly string _location;

    //Not thread safe, but only used in tests
    private readonly List<RecordBatchWithMetadata> _recordBatches = new List<RecordBatchWithMetadata>();

    public FlightSqlHolder(FlightDescriptor flightDescriptor, Schema schema, string location)
    {
        _flightDescriptor = flightDescriptor;
        _schema = schema;
        _location = location;
    }

    public void AddBatch(RecordBatchWithMetadata recordBatchWithMetadata)
    {
        //Should validate schema here
        _recordBatches.Add(recordBatchWithMetadata);
    }

    public IEnumerable<RecordBatchWithMetadata> GetRecordBatches()
    {
        return _recordBatches.ToList();
    }

    public FlightInfo GetFlightInfo()
    {
        int batchArrayLength = _recordBatches.Sum(rb => rb.RecordBatch.Length);
        int batchBytes =
            _recordBatches.Sum(rb => rb.RecordBatch.Arrays.Sum(arr => arr.Data.Buffers.Sum(b => b.Length)));
        var flightInfo = new FlightInfo(_schema, _flightDescriptor,
            new List<FlightEndpoint>()
            {
                new FlightEndpoint(new FlightTicket(_flightDescriptor.Paths.FirstOrDefault() ?? "test"),
                    new List<FlightLocation>() { new FlightLocation(_location) })
            }, batchArrayLength, batchBytes);
        return flightInfo;
    }
}

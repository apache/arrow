using System.Linq;
using Apache.Arrow.Flight.Sql.TestWeb;

namespace Apache.Arrow.Flight.Sql.Tests;

public class FlightSqlTestUtils
{
    private readonly TestSqlWebFactory _testWebFactory;
    private readonly FlightSqlStore _flightStore;

    public FlightSqlTestUtils(TestSqlWebFactory testWebFactory, FlightSqlStore flightStore)
    {
        _testWebFactory = testWebFactory;
        _flightStore = flightStore;
    }

    public RecordBatch CreateTestBatch(int startValue, int length)
    {
        var batchBuilder = new RecordBatch.Builder();
        Int32Array.Builder builder = new();
        for (int i = 0; i < length; i++)
        {
            builder.Append(startValue + i);
        }

        batchBuilder.Append("test", true, builder.Build());
        return batchBuilder.Build();
    }


    public FlightInfo GivenStoreBatches(FlightDescriptor flightDescriptor,
        params RecordBatchWithMetadata[] batches)
    {
        var initialBatch = batches.FirstOrDefault();

        var flightHolder = new FlightSqlHolder(flightDescriptor, initialBatch.RecordBatch.Schema,
            _testWebFactory.GetAddress());

        foreach (var batch in batches)
        {
            flightHolder.AddBatch(batch);
        }

        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        return flightHolder.GetFlightInfo();
    }
}

using System.Linq;
using Apache.Arrow.Flight.TestWeb;

namespace Apache.Arrow.Flight.Tests;

public class FlightTestUtils
{
    private readonly TestWebFactory _testWebFactory;
    private readonly FlightStore _flightStore;

    public FlightTestUtils(TestWebFactory testWebFactory, FlightStore flightStore)
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

        var flightHolder = new FlightHolder(flightDescriptor, initialBatch.RecordBatch.Schema,
            _testWebFactory.GetAddress());

        foreach (var batch in batches)
        {
            flightHolder.AddBatch(batch);
        }

        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        return flightHolder.GetFlightInfo();
    }
}

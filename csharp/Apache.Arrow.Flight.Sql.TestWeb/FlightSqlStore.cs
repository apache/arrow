using System.Collections.Generic;

namespace Apache.Arrow.Flight.Sql.TestWeb;

public class FlightSqlStore
{
    public Dictionary<FlightDescriptor, FlightSqlHolder> Flights { get; set; } = new();
}

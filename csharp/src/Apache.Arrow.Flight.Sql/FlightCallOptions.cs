using Grpc.Core;

namespace Apache.Arrow.Flight.Sql;

public class FlightCallOptions
{
    // Implement any necessary options for RPC calls
    public Metadata Headers { get; set; } = new();

}

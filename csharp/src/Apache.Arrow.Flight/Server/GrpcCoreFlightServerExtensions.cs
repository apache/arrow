#if NET46_OR_GREATER

using Apache.Arrow.Flight.Protocol;
using Apache.Arrow.Flight.Server.Internal;
using Grpc.Core;

namespace Apache.Arrow.Flight.Server
{
    public static class GrpcCoreFlightServerExtensions
    {
        /// <summary>
        /// Create a ServerServiceDefinition for use with a <see href="https://grpc.github.io/grpc/csharp/api/Grpc.Core.Server.html">Grpc.Core Server</see>
        //  This allows running a flight server on pre-Kestrel .net Framework versions
        /// </summary>
        /// <param name="flightServer"></param>
        /// <returns></returns>
        public static ServerServiceDefinition CreateServiceDefinition(this FlightServer flightServer)
        {
            return FlightService.BindService(new FlightServerImplementation(flightServer));
        }
    }
}

#endif

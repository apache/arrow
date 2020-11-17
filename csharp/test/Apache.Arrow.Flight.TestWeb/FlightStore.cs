using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Apache.Arrow.Flight.TestWeb
{
    public class FlightStore
    {
        public Dictionary<FlightDescriptor, FlightHolder> Flights { get; set; } = new Dictionary<FlightDescriptor, FlightHolder>();
    }
}

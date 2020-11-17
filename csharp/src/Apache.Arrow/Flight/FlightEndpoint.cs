using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Arrow.Flight
{
    public class FlightEndpoint
    {
        private readonly Ticket _ticket;
        private readonly IReadOnlyList<Location> _locations;
        internal FlightEndpoint(Protocol.FlightEndpoint flightEndpoint)
        {
            _ticket = new Ticket(flightEndpoint.Ticket);
            _locations = flightEndpoint.Location.Select(x => new Location(x)).ToList();
        }

        public FlightEndpoint(Ticket ticket, IReadOnlyList<Location> locations)
        {
            _ticket = ticket;
            _locations = locations;
        }

        public Ticket Ticket => _ticket;

        public IEnumerable<Location> Locations => _locations;

        public Protocol.FlightEndpoint ToProtocol()
        {
            var output = new Protocol.FlightEndpoint()
            {
                Ticket = _ticket.ToProtocol()
            };

            foreach(var location in _locations)
            {
                output.Location.Add(location.ToProtocol());
            }
            return output;
        }
    }
}

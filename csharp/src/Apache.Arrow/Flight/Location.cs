using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Flight
{
    public class Location
    {
        private readonly Protocol.Location _location;
        public Location(Protocol.Location location)
        {
            _location = location;
        }

        public Location(string uri)
        {
            _location = new Protocol.Location()
            {
                Uri = uri
            };
        }

        public Protocol.Location ToProtocol()
        {
            return _location;
        }
    }
}

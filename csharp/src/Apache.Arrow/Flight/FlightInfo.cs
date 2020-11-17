using System;
using System.Collections.Generic;
using System.Text;
using Apache.Arrow.Flight.Writer;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Flight
{
    public class FlightInfo
    {
        private readonly Schema _schema;
        private readonly FlightDescriptor _flightDescriptor;
        private readonly IList<FlightEndpoint> _flightEndpoints;
        
        public FlightInfo(Protocol.FlightInfo flightInfo)
        {
            _schema = FlightMessageSerializer.DecodeSchema(flightInfo.Schema.Memory);
            _flightDescriptor = new FlightDescriptor(flightInfo.FlightDescriptor);

            var endpoints = new List<FlightEndpoint>();
            foreach(var endpoint in flightInfo.Endpoint)
            {
                endpoints.Add(new FlightEndpoint(endpoint));
            }
            _flightEndpoints = endpoints;
        }

        public FlightInfo(Schema schema, FlightDescriptor flightDescriptor, IList<FlightEndpoint> flightEndpoints)
        {
            _schema = schema;
            _flightDescriptor = flightDescriptor;
            _flightEndpoints = flightEndpoints;
        }

        public IEnumerable<FlightEndpoint> Endpoints => _flightEndpoints;

        public Protocol.FlightInfo ToProtocol()
        {
            var serializedSchema = SchemaWriter.SerializeSchema(_schema);
            var response = new Protocol.FlightInfo()
            {
                Schema = serializedSchema,
                FlightDescriptor = _flightDescriptor.ToProtocol()
            };

            foreach(var endpoint in _flightEndpoints)
            {
                response.Endpoint.Add(endpoint.ToProtocol());
            }

            return response;
        }
    }
}

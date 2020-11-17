using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Apache.Arrow.Flight.TestWeb
{
    public class FlightHolder
    {
        private readonly FlightDescriptor _flightDescriptor;
        private readonly Schema _schema;

        //Not thread safe, but only used in tests
        private readonly List<RecordBatch> _recordBatches = new List<RecordBatch>();
        
        public FlightHolder(FlightDescriptor flightDescriptor, Schema schema)
        {
            _flightDescriptor = flightDescriptor;
            _schema = schema;
        }

        public void AddBatch(RecordBatch recordBatch)
        {
            //Should validate schema here
            _recordBatches.Add(recordBatch);
        }

        public IEnumerable<RecordBatch> GetRecordBatches()
        {
            return _recordBatches.ToList();
        }

        public FlightInfo GetFlightInfo()
        {
            return new FlightInfo(_schema, _flightDescriptor, new List<FlightEndpoint>()
            {
                new FlightEndpoint(new Ticket(_flightDescriptor.Paths.FirstOrDefault()), new List<Location>())
            });
        }
    }
}

// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf;

namespace Apache.Arrow.Flight.TestWeb
{
    public class FlightHolder
    {
        private readonly FlightDescriptor _flightDescriptor;
        private readonly Schema _schema;
        private readonly string _location;

        //Not thread safe, but only used in tests
        private readonly List<RecordBatchWithMetadata> _recordBatches = new List<RecordBatchWithMetadata>();
        
        public FlightHolder(FlightDescriptor flightDescriptor, Schema schema, string location)
        {
            _flightDescriptor = flightDescriptor;
            _schema = schema;
            _location = location;
        }

        public void AddBatch(RecordBatchWithMetadata recordBatchWithMetadata)
        {
            //Should validate schema here
            _recordBatches.Add(recordBatchWithMetadata);
        }

        public IEnumerable<RecordBatchWithMetadata> GetRecordBatches()
        {
            return _recordBatches.ToList();
        }

        public FlightInfo GetFlightInfo()
        {
            int batchArrayLength = _recordBatches.Sum(rb => rb.RecordBatch.Length);
            int batchBytes = _recordBatches.Sum(rb => rb.RecordBatch.Arrays.Sum(arr => arr.Data.Buffers.Sum(b=>b.Length)));
            return new FlightInfo(_schema, _flightDescriptor, new List<FlightEndpoint>()
            {
                new FlightEndpoint(new FlightTicket(_flightDescriptor.Paths.FirstOrDefault()), new List<FlightLocation>(){
                    new FlightLocation(_location)
                })
            }, batchArrayLength, batchBytes);
        }
    }
}

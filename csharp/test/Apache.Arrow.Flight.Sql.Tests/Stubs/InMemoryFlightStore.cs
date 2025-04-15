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

using Apache.Arrow.Flight.TestWeb;
using Apache.Arrow.Types;

namespace Apache.Arrow.Flight.Sql.Tests.Stubs;

public class InMemoryFlightStore : FlightStore
{
    public InMemoryFlightStore()
    {
        // Pre-register a dummy flight so GetFlightInfo can resolve it
        var descriptor = FlightDescriptor.CreatePathDescriptor("test");
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("name").DataType(StringType.Default))
            .Build();

        var recordBatch = new RecordBatch(schema, new Array[]
        {
            new Int32Array.Builder().Append(1).Build(),
            new StringArray.Builder().Append("John Doe").Build()
        }, 1);

        var location = new FlightLocation("grpc+tcp://localhost:50051");

        var flightHolder = new FlightHolder(descriptor, schema, location.Uri);
        flightHolder.AddBatch(new RecordBatchWithMetadata(recordBatch));
        Flights.Add(descriptor, flightHolder);
    }

    public override string ToString()
    {
        return $"InMemoryFlightStore(Flights={Flights.Count})";
    }
}
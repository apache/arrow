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

using System.Linq;
using Apache.Arrow.Flight.Tests;
using Apache.Arrow.Flight.TestWeb;

namespace Apache.Arrow.Flight.Sql.Tests;

public class FlightSqlTestUtils
{
    private readonly TestFlightSqlWebFactory _testWebFactory;
    private readonly FlightStore _flightStore;

    public FlightSqlTestUtils(TestFlightSqlWebFactory testWebFactory, FlightStore flightStore)
    {
        _testWebFactory = testWebFactory;
        _flightStore = flightStore;
    }

    public RecordBatch CreateTestBatch(int startValue, int length) 
    {
        var batchBuilder = new RecordBatch.Builder();
        Int32Array.Builder builder = new();
        for (int i = 0; i < length; i++)
        {
            builder.Append(startValue + i);
        }

        batchBuilder.Append("test", true, builder.Build());
        return batchBuilder.Build();
    }

    public FlightInfo GivenStoreBatches(FlightDescriptor flightDescriptor,
        params RecordBatchWithMetadata[] batches)
    {
        var initialBatch = batches.FirstOrDefault();

        var flightHolder = new FlightHolder(flightDescriptor, initialBatch.RecordBatch.Schema,
            _testWebFactory.GetAddress());

        foreach (var batch in batches)
        {
            flightHolder.AddBatch(batch);
        }

        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        return flightHolder.GetFlightInfo();
    }
}

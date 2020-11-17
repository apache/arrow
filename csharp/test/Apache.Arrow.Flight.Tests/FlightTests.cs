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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.TestWeb;
using Apache.Arrow.Tests;
using Grpc.Core.Utils;
using Xunit;

namespace Apache.Arrow.Flight.Tests
{
    public class FlightTests : IDisposable
    {
        readonly TestWebFactory _testWebFactory;
        readonly FlightClient _flightClient;
        readonly FlightStore _flightStore;
        public FlightTests()
        {
            _flightStore = new FlightStore();
            _testWebFactory = new TestWebFactory(_flightStore);
            _flightClient = new FlightClient(_testWebFactory.GetChannel());
        }

        public void Dispose()
        {
            _testWebFactory.Dispose();
        }

        private RecordBatch CreateTestBatch(int startValue, int length)
        {
            var batchBuilder = new RecordBatch.Builder();
            Int32Array.Builder builder = new Int32Array.Builder();
            for (int i = 0; i < length; i++)
            {
                builder.Append(startValue + i);
            }
            batchBuilder.Append("test", true, builder.Build());
            return batchBuilder.Build();
        }


        private IEnumerable<RecordBatch> GetStoreBatch(FlightDescriptor flightDescriptor)
        {
            Assert.Contains(flightDescriptor, (IReadOnlyDictionary<FlightDescriptor, FlightHolder>)_flightStore.Flights);

            var flightHolder = _flightStore.Flights[flightDescriptor];
            return flightHolder.GetRecordBatches();
        }

        private void GivenStoreBatches(FlightDescriptor flightDescriptor, params RecordBatch[] batches)
        {
            var initialBatch = batches.FirstOrDefault();

            var flightHolder = new FlightHolder(flightDescriptor, initialBatch.Schema);

            foreach(var batch in batches)
            {
                flightHolder.AddBatch(batch);
            }

            _flightStore.Flights.Add(flightDescriptor, flightHolder);
        }

        [Fact]
        public async Task TestPutSingleRecordBatch()
        {
            var flightDescriptor = FlightDescriptor.Path("test");
            var expectedBatch = CreateTestBatch(0, 100);

            var putStream = _flightClient.StartPut(flightDescriptor);
            await putStream.RequestStream.WriteAsync(expectedBatch);
            await putStream.RequestStream.CompleteAsync();
            var putResults = await putStream.ResponseStream.ToListAsync();

            Assert.Single(putResults);

            var actualBatches = GetStoreBatch(flightDescriptor);
            Assert.Single(actualBatches);

            ArrowReaderVerifier.CompareBatches(expectedBatch, actualBatches.First());
        }

        [Fact]
        public async Task TestPutTwoRecordBatches()
        {
            var flightDescriptor = FlightDescriptor.Path("test");
            var expectedBatch1 = CreateTestBatch(0, 100);
            var expectedBatch2 = CreateTestBatch(0, 100);

            var putStream = _flightClient.StartPut(flightDescriptor);
            await putStream.RequestStream.WriteAsync(expectedBatch1);
            await putStream.RequestStream.WriteAsync(expectedBatch2);
            await putStream.RequestStream.CompleteAsync();
            var putResults = await putStream.ResponseStream.ToListAsync();

            Assert.Equal(2, putResults.Count);

            var actualBatches = GetStoreBatch(flightDescriptor).ToList();
            Assert.Equal(2, actualBatches.Count);

            ArrowReaderVerifier.CompareBatches(expectedBatch1, actualBatches[0]);
            ArrowReaderVerifier.CompareBatches(expectedBatch2, actualBatches[1]);
        }

        [Fact]
        public async Task TestGetSingleRecordBatch()
        {
            var flightDescriptor = FlightDescriptor.Path("test");
            var expectedBatch = CreateTestBatch(0, 100);

            //Add batch to the in memory store
            GivenStoreBatches(flightDescriptor, expectedBatch);

            //Get the flight info for the ticket
            var flightInfo = await _flightClient.GetInfo(flightDescriptor);
            Assert.Single(flightInfo.Endpoints);

            var endpoint = flightInfo.Endpoints.FirstOrDefault();

            var getStream = _flightClient.GetStream(endpoint.Ticket);
            var resultList = await getStream.ToListAsync();

            Assert.Single(resultList);
            ArrowReaderVerifier.CompareBatches(expectedBatch, resultList[0]);
        }

        [Fact]
        public async Task TestGetTwoRecordBatch()
        {
            var flightDescriptor = FlightDescriptor.Path("test");
            var expectedBatch1 = CreateTestBatch(0, 100);
            var expectedBatch2 = CreateTestBatch(100, 100);

            //Add batch to the in memory store
            GivenStoreBatches(flightDescriptor, expectedBatch1, expectedBatch2);

            //Get the flight info for the ticket
            var flightInfo = await _flightClient.GetInfo(flightDescriptor);
            Assert.Single(flightInfo.Endpoints);

            var endpoint = flightInfo.Endpoints.FirstOrDefault();

            var getStream = _flightClient.GetStream(endpoint.Ticket);
            var resultList = await getStream.ToListAsync();

            Assert.Equal(2, resultList.Count);
            ArrowReaderVerifier.CompareBatches(expectedBatch1, resultList[0]);
            ArrowReaderVerifier.CompareBatches(expectedBatch2, resultList[1]);
        }

        [Fact]
        public async Task TestListActions()
        {
            var expected = new List<ActionType>()
            {
                new ActionType("get", "get a flight"),
                new ActionType("put", "add a flight"),
                new ActionType("delete", "delete a flight")
            };

            var actual = await _flightClient.ListActions().ToListAsync();

            Assert.Equal(expected, actual);
        }

        
    }
}

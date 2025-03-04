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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.TestWeb;
using Apache.Arrow.Tests;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Core.Utils;
using Microsoft.Extensions.DependencyInjection;
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

        private Schema GetStoreSchema(FlightDescriptor flightDescriptor)
        {
            Assert.Contains(flightDescriptor, (IReadOnlyDictionary<FlightDescriptor, FlightHolder>)_flightStore.Flights);

            var flightHolder = _flightStore.Flights[flightDescriptor];
            return flightHolder.GetFlightInfo().Schema;
        }

        private IEnumerable<RecordBatchWithMetadata> GetStoreBatch(FlightDescriptor flightDescriptor)
        {
            Assert.Contains(flightDescriptor, (IReadOnlyDictionary<FlightDescriptor, FlightHolder>)_flightStore.Flights);

            var flightHolder = _flightStore.Flights[flightDescriptor];
            return flightHolder.GetRecordBatches();
        }

        private FlightInfo GivenStoreBatches(FlightDescriptor flightDescriptor, params RecordBatchWithMetadata[] batches)
        {
            var initialBatch = batches.FirstOrDefault();

            var flightHolder = new FlightHolder(flightDescriptor, initialBatch?.RecordBatch.Schema, _testWebFactory.GetAddress());

            foreach (var batch in batches)
            {
                flightHolder.AddBatch(batch);
            }

            _flightStore.Flights[flightDescriptor] = flightHolder;

            return flightHolder.GetFlightInfo();
        }

        [Fact]
        public async Task TestPutSingleRecordBatch()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("test");
            var expectedBatch = CreateTestBatch(0, 100);

            var putStream = await _flightClient.StartPut(flightDescriptor, expectedBatch.Schema);
            await putStream.RequestStream.WriteAsync(expectedBatch);
            await putStream.RequestStream.CompleteAsync();
            var putResults = await putStream.ResponseStream.ToListAsync();

            Assert.Single(putResults);

            var actualBatches = GetStoreBatch(flightDescriptor);
            Assert.Single(actualBatches);

            ArrowReaderVerifier.CompareBatches(expectedBatch, actualBatches.First().RecordBatch);
        }

        [Fact]
        public async Task TestPutTwoRecordBatches()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("test");
            var expectedBatch1 = CreateTestBatch(0, 100);
            var expectedBatch2 = CreateTestBatch(0, 100);

            var putStream = await _flightClient.StartPut(flightDescriptor, expectedBatch1.Schema);
            await putStream.RequestStream.WriteAsync(expectedBatch1);
            await putStream.RequestStream.WriteAsync(expectedBatch2);
            await putStream.RequestStream.CompleteAsync();
            var putResults = await putStream.ResponseStream.ToListAsync();

            Assert.Equal(2, putResults.Count);

            var actualBatches = GetStoreBatch(flightDescriptor).ToList();
            Assert.Equal(2, actualBatches.Count);

            ArrowReaderVerifier.CompareBatches(expectedBatch1, actualBatches[0].RecordBatch);
            ArrowReaderVerifier.CompareBatches(expectedBatch2, actualBatches[1].RecordBatch);
        }

        [Fact]
        public async Task TestPutZeroRecordBatches()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("test");
            var schema = CreateTestBatch(0, 1).Schema;

            var putStream = await _flightClient.StartPut(flightDescriptor, schema);
            await putStream.RequestStream.CompleteAsync();
            var putResults = await putStream.ResponseStream.ToListAsync();

            Assert.Empty(putResults);

            var actualSchema = GetStoreSchema(flightDescriptor);

            SchemaComparer.Compare(schema, actualSchema);
        }

        [Fact]
        public async Task TestGetRecordBatchWithDelayedSchema()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("test");
            var expectedBatch = CreateTestBatch(0, 100);

            //Add flight info only to the in memory store without schema or batch
            GivenStoreBatches(flightDescriptor);

            //Get the flight info for the ticket and verify the schema is null
            var flightInfo = await _flightClient.GetInfo(flightDescriptor);
            Assert.Single(flightInfo.Endpoints);
            Assert.Null(flightInfo.Schema);

            var endpoint = flightInfo.Endpoints.FirstOrDefault();

            //Update the store with the batch and schema
            GivenStoreBatches(flightDescriptor, new RecordBatchWithMetadata(expectedBatch));
            var getStream = _flightClient.GetStream(endpoint.Ticket);
            var resultList = await getStream.ResponseStream.ToListAsync();

            Assert.Single(resultList);
            ArrowReaderVerifier.CompareBatches(expectedBatch, resultList[0]);
        }

        [Fact]
        public async Task TestGetSingleRecordBatch()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("test");
            var expectedBatch = CreateTestBatch(0, 100);

            //Add batch to the in memory store
            GivenStoreBatches(flightDescriptor, new RecordBatchWithMetadata(expectedBatch));

            //Get the flight info for the ticket
            var flightInfo = await _flightClient.GetInfo(flightDescriptor);
            Assert.Single(flightInfo.Endpoints);

            var endpoint = flightInfo.Endpoints.FirstOrDefault();

            var getStream = _flightClient.GetStream(endpoint.Ticket);
            var resultList = await getStream.ResponseStream.ToListAsync();

            Assert.Single(resultList);
            ArrowReaderVerifier.CompareBatches(expectedBatch, resultList[0]);
        }

        [Fact]
        public async Task TestGetTwoRecordBatch()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("test");
            var expectedBatch1 = CreateTestBatch(0, 100);
            var expectedBatch2 = CreateTestBatch(100, 100);

            //Add batch to the in memory store
            GivenStoreBatches(flightDescriptor, new RecordBatchWithMetadata(expectedBatch1), new RecordBatchWithMetadata(expectedBatch2));

            //Get the flight info for the ticket
            var flightInfo = await _flightClient.GetInfo(flightDescriptor);
            Assert.Single(flightInfo.Endpoints);

            var endpoint = flightInfo.Endpoints.FirstOrDefault();

            var getStream = _flightClient.GetStream(endpoint.Ticket);
            var resultList = await getStream.ResponseStream.ToListAsync();

            Assert.Equal(2, resultList.Count);
            ArrowReaderVerifier.CompareBatches(expectedBatch1, resultList[0]);
            ArrowReaderVerifier.CompareBatches(expectedBatch2, resultList[1]);
        }

        [Fact]
        public async Task TestGetFlightMetadata()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("test");
            var expectedBatch1 = CreateTestBatch(0, 100);

            var expectedMetadata = ByteString.CopyFromUtf8("test metadata");
            var expectedMetadataList = new List<ByteString>() { expectedMetadata };

            //Add batch to the in memory store
            GivenStoreBatches(flightDescriptor, new RecordBatchWithMetadata(expectedBatch1, expectedMetadata));

            //Get the flight info for the ticket
            var flightInfo = await _flightClient.GetInfo(flightDescriptor);
            Assert.Single(flightInfo.Endpoints);

            var endpoint = flightInfo.Endpoints.FirstOrDefault();

            var getStream = _flightClient.GetStream(endpoint.Ticket);

            List<ByteString> actualMetadata = new List<ByteString>();
            while (await getStream.ResponseStream.MoveNext(default))
            {
                actualMetadata.AddRange(getStream.ResponseStream.ApplicationMetadata);
            }

            Assert.Equal(expectedMetadataList, actualMetadata);
        }

        [Fact]
        public async Task TestPutWithMetadata()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("test");
            var expectedBatch = CreateTestBatch(0, 100);
            var expectedMetadata = ByteString.CopyFromUtf8("test metadata");

            var putStream = await _flightClient.StartPut(flightDescriptor, expectedBatch.Schema);
            await putStream.RequestStream.WriteAsync(expectedBatch, expectedMetadata);
            await putStream.RequestStream.CompleteAsync();
            var putResults = await putStream.ResponseStream.ToListAsync();

            Assert.Single(putResults);

            var actualBatches = GetStoreBatch(flightDescriptor);
            Assert.Single(actualBatches);

            ArrowReaderVerifier.CompareBatches(expectedBatch, actualBatches.First().RecordBatch);
            Assert.Equal(expectedMetadata, actualBatches.First().Metadata);
        }

        [Fact]
        public async Task TestGetSchema()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("test");
            var expectedBatch = CreateTestBatch(0, 100);
            var expectedSchema = expectedBatch.Schema;

            GivenStoreBatches(flightDescriptor, new RecordBatchWithMetadata(expectedBatch));

            var actualSchema = await _flightClient.GetSchema(flightDescriptor);

            SchemaComparer.Compare(expectedSchema, actualSchema);
        }

        [Fact]
        public async Task TestDoAction()
        {
            var expectedResult = new List<FlightResult>()
            {
                new FlightResult("test data")
            };

            var resultStream = _flightClient.DoAction(new FlightAction("test"));
            var actualResult = await resultStream.ResponseStream.ToListAsync();

            Assert.Equal(expectedResult, actualResult);
        }

        [Fact]
        public async Task TestListActions()
        {
            var expected = new List<FlightActionType>()
            {
                new FlightActionType("get", "get a flight"),
                new FlightActionType("put", "add a flight"),
                new FlightActionType("delete", "delete a flight"),
                new FlightActionType("test", "test action")
            };

            var actual = await _flightClient.ListActions().ResponseStream.ToListAsync();

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task TestListFlights()
        {
            var flightDescriptor1 = FlightDescriptor.CreatePathDescriptor("test1");
            var flightDescriptor2 = FlightDescriptor.CreatePathDescriptor("test2");
            var expectedBatch = CreateTestBatch(0, 100);

            List<FlightInfo> expectedFlightInfo = new List<FlightInfo>();

            expectedFlightInfo.Add(GivenStoreBatches(flightDescriptor1, new RecordBatchWithMetadata(expectedBatch)));
            expectedFlightInfo.Add(GivenStoreBatches(flightDescriptor2, new RecordBatchWithMetadata(expectedBatch)));

            var listFlightStream = _flightClient.ListFlights();

            var actualFlights = await listFlightStream.ResponseStream.ToListAsync();

            for (int i = 0; i < expectedFlightInfo.Count; i++)
            {
                FlightInfoComparer.Compare(expectedFlightInfo[i], actualFlights[i]);
            }
        }

        [Fact]
        public async Task TestHandshake()
        {
            var duplexStreamingCall = _flightClient.Handshake();

            await duplexStreamingCall.RequestStream.WriteAsync(new FlightHandshakeRequest(ByteString.Empty));
            await duplexStreamingCall.RequestStream.CompleteAsync();
            var results = await duplexStreamingCall.ResponseStream.ToListAsync();

            Assert.Single(results);
            Assert.Equal("Done", results.First().Payload.ToStringUtf8());
        }

        [Fact]
        public async Task TestSingleExchange()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("single_exchange");
            var duplexStreamingCall = _flightClient.DoExchange(flightDescriptor);
            var expectedBatch = CreateTestBatch(0, 100);

            await duplexStreamingCall.RequestStream.WriteAsync(expectedBatch);
            await duplexStreamingCall.RequestStream.CompleteAsync();

            var results = await duplexStreamingCall.ResponseStream.ToListAsync();

            Assert.Single(results);
            ArrowReaderVerifier.CompareBatches(expectedBatch, results.FirstOrDefault());
        }

        [Fact]
        public async Task TestMultipleExchange()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("multiple_exchange");
            var duplexStreamingCall = _flightClient.DoExchange(flightDescriptor);
            var expectedBatch1 = CreateTestBatch(0, 100);
            var expectedBatch2 = CreateTestBatch(100, 100);

            await duplexStreamingCall.RequestStream.WriteAsync(expectedBatch1);
            await duplexStreamingCall.RequestStream.WriteAsync(expectedBatch2);
            await duplexStreamingCall.RequestStream.CompleteAsync();

            var results = await duplexStreamingCall.ResponseStream.ToListAsync();

            ArrowReaderVerifier.CompareBatches(expectedBatch1, results[0]);
            ArrowReaderVerifier.CompareBatches(expectedBatch2, results[1]);
        }

        [Fact]
        public async Task TestExchangeWithMetadata()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("metadata_exchange");
            var duplexStreamingCall = _flightClient.DoExchange(flightDescriptor);
            var expectedBatch = CreateTestBatch(0, 100);
            var expectedMetadata = ByteString.CopyFromUtf8("test metadata");

            await duplexStreamingCall.RequestStream.WriteAsync(expectedBatch, expectedMetadata);
            await duplexStreamingCall.RequestStream.CompleteAsync();

            List<ByteString> actualMetadata = new List<ByteString>();
            List<RecordBatch> actualBatch = new List<RecordBatch>();
            while (await duplexStreamingCall.ResponseStream.MoveNext(default))
            {
                actualBatch.Add(duplexStreamingCall.ResponseStream.Current);
                actualMetadata.AddRange(duplexStreamingCall.ResponseStream.ApplicationMetadata);
            }

            ArrowReaderVerifier.CompareBatches(expectedBatch, actualBatch.FirstOrDefault());
            Assert.Equal(expectedMetadata, actualMetadata.FirstOrDefault());
        }

        [Fact]
        public async Task TestHandshakeWithSpecificMessage()
        {
            var duplexStreamingCall = _flightClient.Handshake();

            await duplexStreamingCall.RequestStream.WriteAsync(new FlightHandshakeRequest(ByteString.CopyFromUtf8("Hello")));
            await duplexStreamingCall.RequestStream.CompleteAsync();
            var results = await duplexStreamingCall.ResponseStream.ToListAsync();

            Assert.Single(results);
            Assert.Equal("Hello handshake", results.First().Payload.ToStringUtf8());
        }

        [Fact]
        public async Task TestGetBatchesWithAsyncEnumerable()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("test");
            var expectedBatch1 = CreateTestBatch(0, 100);
            var expectedBatch2 = CreateTestBatch(100, 100);

            //Add batch to the in memory store
            GivenStoreBatches(flightDescriptor, new RecordBatchWithMetadata(expectedBatch1), new RecordBatchWithMetadata(expectedBatch2));

            //Get the flight info for the ticket
            var flightInfo = await _flightClient.GetInfo(flightDescriptor);
            Assert.Single(flightInfo.Endpoints);

            var endpoint = flightInfo.Endpoints.FirstOrDefault();

            var getStream = _flightClient.GetStream(endpoint.Ticket);


            List<RecordBatch> resultList = new List<RecordBatch>();
            await foreach (var recordBatch in getStream.ResponseStream)
            {
                resultList.Add(recordBatch);
            }

            Assert.Equal(2, resultList.Count);
            ArrowReaderVerifier.CompareBatches(expectedBatch1, resultList[0]);
            ArrowReaderVerifier.CompareBatches(expectedBatch2, resultList[1]);
        }

        [Fact]
        public async Task EnsureTheSerializedBatchContainsTheProperTotalRecordsAndTotalBytesProperties()
        {
            var flightDescriptor1 = FlightDescriptor.CreatePathDescriptor("test1");
            var expectedBatch = CreateTestBatch(0, 100);
            var expectedTotalBytes = expectedBatch.Arrays.Sum(arr => arr.Data.Buffers.Sum(b => b.Length));

            List<FlightInfo> expectedFlightInfo = new List<FlightInfo>();

            expectedFlightInfo.Add(GivenStoreBatches(flightDescriptor1, new RecordBatchWithMetadata(expectedBatch)));

            var listFlightStream = _flightClient.ListFlights();

            var actualFlights = await listFlightStream.ResponseStream.ToListAsync();
            var result = actualFlights.First();

            Assert.Equal(expectedBatch.Length, result.TotalRecords);
            Assert.Equal(expectedTotalBytes, result.TotalBytes);
        }

        [Fact]
        public async Task EnsureCallRaisesDeadlineExceeded()
        {
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("raise_deadline");
            var deadline = DateTime.UtcNow;
            var batch = CreateTestBatch(0, 100);

            RpcException exception = null;

            var asyncServerStreamingCallFlights = _flightClient.ListFlights(null, null, deadline);
            Assert.Equal(StatusCode.DeadlineExceeded, asyncServerStreamingCallFlights.GetStatus().StatusCode);

            var asyncServerStreamingCallActions = _flightClient.ListActions(null, deadline);
            Assert.Equal(StatusCode.DeadlineExceeded, asyncServerStreamingCallFlights.GetStatus().StatusCode);

            GivenStoreBatches(flightDescriptor, new RecordBatchWithMetadata(batch));
            exception = await Assert.ThrowsAsync<RpcException>(async () => await _flightClient.GetInfo(flightDescriptor, null, deadline));
            Assert.Equal(StatusCode.DeadlineExceeded, exception.StatusCode);

            var flightInfo = await _flightClient.GetInfo(flightDescriptor);
            var endpoint = flightInfo.Endpoints.FirstOrDefault();
            var getStream = _flightClient.GetStream(endpoint.Ticket, null, deadline);
            Assert.Equal(StatusCode.DeadlineExceeded, getStream.GetStatus().StatusCode);

            var duplexStreamingCall = _flightClient.DoExchange(flightDescriptor, null, deadline);
            exception = await Assert.ThrowsAsync<RpcException>(async () => await duplexStreamingCall.RequestStream.WriteAsync(batch));
            Assert.Equal(StatusCode.DeadlineExceeded, exception.StatusCode);

            exception = await Assert.ThrowsAsync<RpcException>(async () => await _flightClient.StartPut(flightDescriptor, batch.Schema, null, deadline));
            Assert.Equal(StatusCode.DeadlineExceeded, exception.StatusCode);

            exception = await Assert.ThrowsAsync<RpcException>(async () => await _flightClient.GetSchema(flightDescriptor, null, deadline));
            Assert.Equal(StatusCode.DeadlineExceeded, exception.StatusCode);

            var handshakeStreamingCall = _flightClient.Handshake(null, deadline);
            exception = await Assert.ThrowsAsync<RpcException>(async () => await handshakeStreamingCall.RequestStream.WriteAsync(new FlightHandshakeRequest(ByteString.Empty)));
            Assert.Equal(StatusCode.DeadlineExceeded, exception.StatusCode);
        }

        [Fact]
        public async Task EnsureCallRaisesRequestCancelled()
        {
            var cts = new CancellationTokenSource();
            cts.CancelAfter(1);
            
            var batch = CreateTestBatch(0, 100);
            var metadata = new Metadata();
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("raise_cancelled");
            await Task.Delay(5);
            RpcException exception = null;

            var asyncServerStreamingCallFlights = _flightClient.ListFlights(null, null, null, cts.Token);
            Assert.Equal(StatusCode.Cancelled, asyncServerStreamingCallFlights.GetStatus().StatusCode);

            var asyncServerStreamingCallActions = _flightClient.ListActions(null, null, cts.Token);
            Assert.Equal(StatusCode.Cancelled, asyncServerStreamingCallFlights.GetStatus().StatusCode);

            GivenStoreBatches(flightDescriptor, new RecordBatchWithMetadata(batch));
            exception = await Assert.ThrowsAsync<RpcException>(async () => await _flightClient.GetInfo(flightDescriptor, null, null, cts.Token));
            Assert.Equal(StatusCode.Cancelled, exception.StatusCode);

            var flightInfo = await _flightClient.GetInfo(flightDescriptor);
            var endpoint = flightInfo.Endpoints.FirstOrDefault();
            var getStream = _flightClient.GetStream(endpoint.Ticket, null, null, cts.Token);
            Assert.Equal(StatusCode.Cancelled, getStream.GetStatus().StatusCode);

            var duplexStreamingCall = _flightClient.DoExchange(flightDescriptor, null, null, cts.Token);
            exception = await Assert.ThrowsAsync<RpcException>(async () => await duplexStreamingCall.RequestStream.WriteAsync(batch));
            Assert.Equal(StatusCode.Cancelled, exception.StatusCode);

            exception = await Assert.ThrowsAsync<RpcException>(async () => await _flightClient.StartPut(flightDescriptor, batch.Schema, null, null, cts.Token));
            Assert.Equal(StatusCode.Cancelled, exception.StatusCode);

            exception = await Assert.ThrowsAsync<RpcException>(async () => await _flightClient.GetSchema(flightDescriptor, null, null, cts.Token));
            Assert.Equal(StatusCode.Cancelled, exception.StatusCode);

            var handshakeStreamingCall = _flightClient.Handshake(null, null, cts.Token);
            exception = await Assert.ThrowsAsync<RpcException>(async () => await handshakeStreamingCall.RequestStream.WriteAsync(new FlightHandshakeRequest(ByteString.Empty)));
            Assert.Equal(StatusCode.Cancelled, exception.StatusCode);
        }

        [Fact]
        public async Task TestIntegrationWithGrpcNetClientFactory()
        {
            IServiceCollection services = new ServiceCollection();

            services.AddGrpcClient<FlightClient>(grpc => grpc.Address = new Uri(_testWebFactory.GetAddress()));

            IServiceProvider provider = services.BuildServiceProvider();

            // Test that an instance of the FlightClient can be resolved whilst using the Grpc.Net.ClientFactory library.
            FlightClient flightClient = provider.GetRequiredService<FlightClient>();

            // Test that the resolved client is functional.
            var flightDescriptor = FlightDescriptor.CreatePathDescriptor("test");
            var expectedBatch = CreateTestBatch(0, 100);
            var expectedSchema = expectedBatch.Schema;

            GivenStoreBatches(flightDescriptor, new RecordBatchWithMetadata(expectedBatch));

            var actualSchema = await flightClient.GetSchema(flightDescriptor);

            SchemaComparer.Compare(expectedSchema, actualSchema);
        }
    }
}

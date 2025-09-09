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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.Sql.Client;
using Apache.Arrow.Flight.TestWeb;
using Apache.Arrow.Types;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Grpc.Core;
using Xunit;

namespace Apache.Arrow.Flight.Sql.Tests
{
    public class FlightSqlPreparedStatementTests
    {
        readonly TestFlightSqlWebFactory _testWebFactory;
        readonly FlightStore _flightStore;
        readonly FlightSqlClient _flightSqlClient;
        private readonly PreparedStatement _preparedStatement;
        private readonly Schema _schema;
        private readonly FlightDescriptor _flightDescriptor;
        private readonly RecordBatch _parameterBatch;

        public FlightSqlPreparedStatementTests()
        {
            _flightStore = new FlightStore();
            _testWebFactory = new TestFlightSqlWebFactory(_flightStore);
            _flightSqlClient = new FlightSqlClient(new FlightClient(_testWebFactory.GetChannel()));

            _flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test-query");
            _schema = CreateSchema();
            _parameterBatch = CreateParameterBatch();
            _preparedStatement = new PreparedStatement(_flightSqlClient, "test-handle-guid", _schema, _schema);
        }

        private static Schema CreateSchema()
        {
            return new Schema.Builder()
                .Field(f => f.Name("DATA_TYPE_ID").DataType(Int32Type.Default).Nullable(false))
                .Build();
        }

        private RecordBatch CreateParameterBatch()
        {
            return new RecordBatch(_schema,
                new IArrowArray[]
                {
                    new Int32Array.Builder().AppendRange(new[] { 1, 2, 3 }).Build(),
                    new StringArray.Builder().AppendRange(new[] { "INTEGER", "VARCHAR", "BOOLEAN" }).Build(),
                    new Int32Array.Builder().AppendRange(new[] { 32, 255, 1 }).Build()
                }, 3);
        }
        
        [Fact]
        public async Task ExecuteAsync_ShouldReturnFlightInfo_WhenValidInputsAreProvided()
        {
            var validRecordBatch = CreateParameterBatch();
            _preparedStatement.SetParameters(validRecordBatch);
            var flightInfo = await _preparedStatement.ExecuteAsync();

            Assert.NotNull(flightInfo);
            Assert.IsType<FlightInfo>(flightInfo);
        }

        [Fact]
        public async Task GetSchemaAsync_ShouldThrowInvalidOperationException_WhenStatementIsClosed()
        {
            await _preparedStatement.CloseAsync(new FlightCallOptions());
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                _preparedStatement.GetSchemaAsync(new FlightCallOptions()));
        }

        [Fact]
        public async Task ExecuteUpdateAsync_ShouldReturnAffectedRows_WhenParametersAreSet()
        {
            var affectedRows = await _preparedStatement.ExecuteUpdateAsync(_parameterBatch);
            Assert.True(affectedRows > 0, "Expected affected rows to be greater than 0.");
        }

        [Fact]
        public async Task BindParametersAsync_ShouldReturnMetadata_WhenValidInputsAreProvided()
        {
            var metadata = await _preparedStatement.BindParametersAsync(_flightDescriptor, _parameterBatch);
            Assert.NotNull(metadata);
            Assert.True(metadata.Length > 0, "Metadata should have a length greater than 0 when valid.");
        }

        [Theory]
        [MemberData(nameof(GetTestData))]
        public async Task TestSetParameters(RecordBatch parameterBatch, Schema parameterSchema, Type expectedException)
        {
            var preparedStatement = new PreparedStatement(_flightSqlClient, "TestHandle", _schema, parameterSchema);
            if (expectedException != null)
            {
                var exception =
                    await Record.ExceptionAsync(() => Task.Run(() => preparedStatement.SetParameters(parameterBatch)));
                Assert.NotNull(exception);
                Assert.IsType(expectedException, exception);
            }
        }

        [Fact]
        public async Task TestSetParameters_Cancelled()
        {
            var validRecordBatch = CreateRecordBatch([1, 2, 3]);
            var cts = new CancellationTokenSource();
            await cts.CancelAsync();
            _preparedStatement.SetParameters(validRecordBatch);
        }

        [Fact]
        public async Task TestCloseAsync()
        {
            await _preparedStatement.CloseAsync(new FlightCallOptions());
            Assert.True(_preparedStatement.IsClosed,
                "PreparedStatement should be marked as closed after calling CloseAsync.");
        }

        [Fact]
        public async Task ReadResultAsync_ShouldPopulateMessage_WhenValidFlightData()
        {
            var message = new ActionCreatePreparedStatementResult();
            var flightData = new FlightData(_flightDescriptor, ByteString.CopyFromUtf8("test-data"));
            var results = GetAsyncEnumerable(new List<FlightData> { flightData });

            await _preparedStatement.ReadResultAsync(results, message);
            Assert.NotEmpty(message.PreparedStatementHandle.ToStringUtf8());
        }

        [Fact]
        public async Task ReadResultAsync_ShouldNotThrow_WhenFlightDataBodyIsNullOrEmpty()
        {
            var message = new ActionCreatePreparedStatementResult();
            var flightData1 = new FlightData(_flightDescriptor, ByteString.Empty);
            var flightData2 = new FlightData(_flightDescriptor, ByteString.CopyFromUtf8(""));
            var results = GetAsyncEnumerable(new List<FlightData> { flightData1, flightData2 });

            await _preparedStatement.ReadResultAsync(results, message);
            Assert.Empty(message.PreparedStatementHandle.ToStringUtf8());
        }

        [Fact]
        public async Task ParseResponseAsync_ShouldReturnPreparedStatement_WhenValidData()
        {
            var preparedStatementHandle = "test-handle";
            var actionResult = new ActionCreatePreparedStatementResult
            {
                PreparedStatementHandle = ByteString.CopyFrom(preparedStatementHandle, Encoding.UTF8),
                DatasetSchema = _schema.ToByteString(),
                ParameterSchema = _schema.ToByteString()
            };
            var flightData = new FlightData(_flightDescriptor, ByteString.CopyFrom(actionResult.ToByteArray()));
            var results = GetAsyncEnumerable(new List<FlightData> { flightData });

            var preparedStatement = await _preparedStatement.ParseResponseAsync(_flightSqlClient, results);
            Assert.NotNull(preparedStatement);
            Assert.Equal(preparedStatementHandle, preparedStatement.Handle);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public async Task ParseResponseAsync_ShouldThrowException_WhenPreparedStatementHandleIsNullOrEmpty(
            string handle)
        {
            ActionCreatePreparedStatementResult actionResult = string.IsNullOrEmpty(handle)
                ? new ActionCreatePreparedStatementResult()
                : new ActionCreatePreparedStatementResult
                    { PreparedStatementHandle = ByteString.CopyFrom(handle, Encoding.UTF8) };

            var flightData = new FlightData(_flightDescriptor, ByteString.CopyFrom(actionResult.ToByteArray()));
            var results = GetAsyncEnumerable(new List<FlightData> { flightData });

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                _preparedStatement.ParseResponseAsync(_flightSqlClient, results));
        }

        private async IAsyncEnumerable<T> GetAsyncEnumerable<T>(IEnumerable<T> enumerable)
        {
            foreach (var item in enumerable)
            {
                yield return item;
                await Task.Yield();
            }
        }

        public static IEnumerable<object[]> GetTestData()
        {
            var schema = new Schema.Builder().Field(f => f.Name("field1").DataType(Int32Type.Default)).Build();
            var validRecordBatch = CreateRecordBatch([1, 2, 3]);
            var invalidSchema = new Schema.Builder().Field(f => f.Name("invalid_field").DataType(Int32Type.Default))
                .Build();
            var invalidRecordBatch = CreateRecordBatch([4, 5, 6]);

            return new List<object[]>
            {
                new object[] { validRecordBatch, schema, null },
                new object[] { null, schema, typeof(ArgumentNullException) }
            };
        }

        public static RecordBatch CreateRecordBatch(int[] values)
        {
            var int32Array = new Int32Array.Builder().AppendRange(values).Build();
            return new RecordBatch.Builder().Append("field1", true, int32Array).Build();
        }
    }
}
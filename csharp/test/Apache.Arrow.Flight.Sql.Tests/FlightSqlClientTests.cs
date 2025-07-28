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
using Apache.Arrow.Flight.Sql.Client;
using Apache.Arrow.Flight.TestWeb;
using Apache.Arrow.Types;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Grpc.Core.Utils;
using Xunit;

namespace Apache.Arrow.Flight.Sql.Tests;

public class FlightSqlClientTests : IDisposable
{
    readonly TestFlightSqlWebFactory _testWebFactory;
    readonly FlightStore _flightStore;
    private readonly FlightSqlClient _flightSqlClient;
    private readonly FlightSqlTestUtils _testUtils;

    public FlightSqlClientTests()
    {
        _flightStore = new FlightStore();
        _testWebFactory = new TestFlightSqlWebFactory(_flightStore);
        FlightClient flightClient = new(_testWebFactory.GetChannel());
        _flightSqlClient = new FlightSqlClient(flightClient);

        _testUtils = new FlightSqlTestUtils(_testWebFactory, _flightStore);
    }

    #region Transactions

    [Fact]
    public async Task CommitTransactionAsync()
    {
        // Arrange
        string transactionId = "sample-transaction-id";
        var transaction = new Transaction(transactionId);

        // Act
        var streamCall = _flightSqlClient.CommitAsync(transaction);
        var result = await streamCall.ResponseStream.ToListAsync();

        // Assert
        Assert.NotNull(result);
        Assert.Equal(transaction.TransactionId, result.FirstOrDefault()?.Body);
    }

    [Fact]
    public async Task BeginTransactionAsync()
    {
        // Arrange
        string expectedTransactionId = "sample-transaction-id";

        // Act
        var transaction = await _flightSqlClient.BeginTransactionAsync();

        // Assert
        Assert.NotEqual(Transaction.NoTransaction, transaction);
        Assert.Equal(ByteString.CopyFromUtf8(expectedTransactionId), transaction.TransactionId);
    }

    [Fact]
    public async Task RollbackTransactionAsync()
    {
        // Arrange
        string transactionId = "sample-transaction-id";
        var transaction = new Transaction(transactionId);

        // Act
        var streamCall = _flightSqlClient.RollbackAsync(transaction);
        var result = await streamCall.ResponseStream.ToListAsync();

        // Assert
        Assert.Equal(result.FirstOrDefault()?.Body, transaction.TransactionId);
    }

    #endregion

    #region PreparedStatement

    [Theory]
    [InlineData("sample-transaction-id", true)]
    [InlineData(null, false)]
    public async Task PreparedAsync(string transactionId, bool expectTransaction)
    {
        // Arrange
        string query = "INSERT INTO users (id, name) VALUES (1, 'John Doe')";
        var transaction = string.IsNullOrEmpty(transactionId)
            ? Transaction.NoTransaction
            : new Transaction(transactionId);

        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");

        // Create a sample schema for the dataset and parameters
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("name").DataType(StringType.Default))
            .Build();

        var recordBatch = new RecordBatch(schema, new Array[]
        {
            new Int32Array.Builder().Append(1).Build(),
            new StringArray.Builder().Append("John Doe").Build()
        }, 1);

        var flightHolder = new FlightHolder(flightDescriptor, schema, _testWebFactory.GetAddress());
        flightHolder.AddBatch(new RecordBatchWithMetadata(recordBatch));
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        var datasetSchemaBytes = SchemaExtensions.SerializeSchema(schema);
        var parameterSchemaBytes = SchemaExtensions.SerializeSchema(schema);

        var preparedStatementResponse = new ActionCreatePreparedStatementResult
        {
            PreparedStatementHandle = ByteString.CopyFromUtf8("prepared-handle"),
            DatasetSchema = ByteString.CopyFrom(datasetSchemaBytes),
            ParameterSchema = ByteString.CopyFrom(parameterSchemaBytes)
        };

        // Act
        PreparedStatement preparedStatement;
        if (expectTransaction)
        {
             preparedStatement = await _flightSqlClient.PrepareAsync(query, transaction);
        }
        else
        {
            preparedStatement = await _flightSqlClient.PrepareAsync(query);
        }
        var deserializedDatasetSchema =
            SchemaExtensions.DeserializeSchema(preparedStatementResponse.DatasetSchema.ToByteArray());
        var deserializedParameterSchema =
            SchemaExtensions.DeserializeSchema(preparedStatementResponse.ParameterSchema.ToByteArray());

        // Assert
        Assert.NotNull(preparedStatement);
        Assert.NotNull(deserializedDatasetSchema);
        Assert.NotNull(deserializedParameterSchema);
        CompareSchemas(schema, deserializedDatasetSchema);
        CompareSchemas(schema, deserializedParameterSchema);
    }

    #endregion

    [Fact]
    public async Task ExecuteUpdateAsync()
    {
        // Arrange
        string query = "UPDATE test_table SET column1 = 'value'";
        var transaction = new Transaction("sample-transaction-id");
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("name").DataType(StringType.Default))
            .Build();

        var recordBatch = new RecordBatch(schema, new IArrowArray[]
        {
            new Int32Array.Builder().AppendRange([1, 2, 3, 4, 5]).Build(),
            new StringArray.Builder().AppendRange(["John Doe", "Jane Doe", "Alice", "Bob", "Charlie"]).Build()
        }, 5);

        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema, _testWebFactory.GetAddress());
        flightHolder.AddBatch(new RecordBatchWithMetadata(recordBatch));
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        long affectedRows = await _flightSqlClient.ExecuteUpdateAsync(query, transaction);

        // Assert
        Assert.Equal(5, affectedRows);
    }

    [Fact]
    public async Task ExecuteAsync()
    {
        // Arrange
        string query = "SELECT * FROM test_table";
        var transaction = new Transaction("sample-transaction-id");
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);

        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema, _testWebFactory.GetAddress());
        flightHolder.AddBatch(new RecordBatchWithMetadata(recordBatch));

        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var flightInfo = await _flightSqlClient.ExecuteAsync(query, transaction);

        // Assert
        Assert.NotNull(flightInfo);
        Assert.Single(flightInfo.Endpoints);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldReturnFlightInfo_WhenValidInputsAreProvided()
    {
        // Arrange
        string query = "SELECT * FROM test_table";
        var transaction = new Transaction("sample-transaction-id");
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var flightInfo = await _flightSqlClient.ExecuteAsync(query, transaction);

        // Assert
        Assert.NotNull(flightInfo);
        Assert.IsType<FlightInfo>(flightInfo);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldThrowArgumentException_WhenQueryIsEmpty()
    {
        // Arrange
        string emptyQuery = string.Empty;
        var transaction = new Transaction("sample-transaction-id");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _flightSqlClient.ExecuteAsync(emptyQuery, transaction));
    }

    [Fact]
    public async Task ExecuteAsync_ShouldReturnFlightInfo_WhenTransactionIsNoTransaction()
    {
        // Arrange
        string query = "SELECT * FROM test_table";
        var transaction = Transaction.NoTransaction;
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var flightInfo = await _flightSqlClient.ExecuteAsync(query, transaction);

        // Assert
        Assert.NotNull(flightInfo);
        Assert.IsType<FlightInfo>(flightInfo);
    }

    [Fact]
    public async Task GetFlightInfoAsync()
    {
        // Arrange
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);
        // Act
        var flightInfo = await _flightSqlClient.GetFlightInfoAsync(flightDescriptor);

        // Assert
        Assert.NotNull(flightInfo);
    }

    [Fact]
    public async Task GetExecuteSchemaAsync()
    {
        // Arrange
        string query = "SELECT * FROM test_table";
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        Schema resultSchema =
            await _flightSqlClient.GetExecuteSchemaAsync(query, new Transaction("sample-transaction-id"));

        // Assert
        Assert.NotNull(resultSchema);
        Assert.Equal(recordBatch.Schema.FieldsList.Count, resultSchema.FieldsList.Count);
        CompareSchemas(resultSchema, recordBatch.Schema);
    }

    [Fact]
    public async Task GetCatalogsAsync()
    {
        // Arrange
        var options = new FlightCallOptions();
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var result = await _flightSqlClient.GetCatalogsAsync(options);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(flightHolder.GetFlightInfo().Endpoints.Count, result.Endpoints.Count);
        Assert.Equal(flightDescriptor, result.Descriptor);
    }

    [Fact]
    public async Task GetSchemaAsync()
    {
        // Arrange
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var result = await _flightSqlClient.GetSchemaAsync(flightDescriptor);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(recordBatch.Schema.FieldsList.Count, result.FieldsList.Count);
        CompareSchemas(result, recordBatch.Schema);
    }

    [Fact]
    public async Task GetDbSchemasAsync()
    {
        // Arrange
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);
        string catalog = "test-catalog";
        string dbSchemaFilterPattern = "test-schema-pattern";

        // Act
        var result = await _flightSqlClient.GetDbSchemasAsync(catalog, dbSchemaFilterPattern);

        // Assert
        Assert.NotNull(result);
        var expectedFlightInfo = flightHolder.GetFlightInfo();
        Assert.Equal(recordBatch.Schema.FieldsList.Count, result.Schema.FieldsList.Count);
        Assert.Equal(expectedFlightInfo.Descriptor.Command, result.Descriptor.Command);
        Assert.Equal(expectedFlightInfo.Descriptor.Type, result.Descriptor.Type);
        Assert.Equal(expectedFlightInfo.Schema.FieldsList.Count, result.Schema.FieldsList.Count);
        Assert.Equal(expectedFlightInfo.Endpoints.Count, result.Endpoints.Count);

        for (int i = 0; i < expectedFlightInfo.Schema.FieldsList.Count; i++)
        {
            var expectedField = expectedFlightInfo.Schema.FieldsList[i];
            var actualField = result.Schema.FieldsList[i];

            Assert.Equal(expectedField.Name, actualField.Name);
            Assert.Equal(expectedField.DataType, actualField.DataType);
            Assert.Equal(expectedField.IsNullable, actualField.IsNullable);
            Assert.Equal(expectedField.Metadata?.Count ?? 0, actualField.Metadata?.Count ?? 0);
        }

        for (int i = 0; i < expectedFlightInfo.Endpoints.Count; i++)
        {
            var expectedEndpoint = expectedFlightInfo.Endpoints[i];
            var actualEndpoint = result.Endpoints[i];

            Assert.Equal(expectedEndpoint.Ticket, actualEndpoint.Ticket);
            Assert.Equal(expectedEndpoint.Locations.Count(), actualEndpoint.Locations.Count());
        }
    }

    [Fact]
    public async Task GetPrimaryKeysAsync()
    {
        // Arrange
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var tableRef = new TableRef("test-catalog", "test-schema", "test-table");
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var result = await _flightSqlClient.GetPrimaryKeysAsync(tableRef);

        // Assert
        Assert.NotNull(result);
        var expectedFlightInfo = flightHolder.GetFlightInfo();
        Assert.Equal(expectedFlightInfo.Descriptor.Command, result.Descriptor.Command);
        Assert.Equal(expectedFlightInfo.Descriptor.Type, result.Descriptor.Type);
        Assert.Equal(expectedFlightInfo.Schema.FieldsList.Count, result.Schema.FieldsList.Count);

        for (int i = 0; i < expectedFlightInfo.Schema.FieldsList.Count; i++)
        {
            var expectedField = expectedFlightInfo.Schema.FieldsList[i];
            var actualField = result.Schema.FieldsList[i];
            Assert.Equal(expectedField.Name, actualField.Name);
            Assert.Equal(expectedField.DataType, actualField.DataType);
            Assert.Equal(expectedField.IsNullable, actualField.IsNullable);
            Assert.Equal(expectedField.Metadata?.Count ?? 0, actualField.Metadata?.Count ?? 0);
        }

        Assert.Equal(expectedFlightInfo.Endpoints.Count, result.Endpoints.Count);

        for (int i = 0; i < expectedFlightInfo.Endpoints.Count; i++)
        {
            var expectedEndpoint = expectedFlightInfo.Endpoints[i];
            var actualEndpoint = result.Endpoints[i];

            Assert.Equal(expectedEndpoint.Ticket, actualEndpoint.Ticket);
            Assert.Equal(expectedEndpoint.Locations.Count(), actualEndpoint.Locations.Count());
        }
    }

    [Fact]
    public async Task GetTablesAsync()
    {
        // Arrange
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        string catalog = "sample_catalog";
        string dbSchemaFilterPattern = "sample_schema";
        string tableFilterPattern = "sample_table";
        bool includeSchema = true;
        var tableTypes = new List<string> { "BASE TABLE" };

        // Act
        var result = await _flightSqlClient.GetTablesAsync(catalog, dbSchemaFilterPattern, tableFilterPattern,
            includeSchema, tableTypes);

        // Assert
        Assert.NotNull(result);
        Assert.Single(result);

        var expectedFlightInfo = flightHolder.GetFlightInfo();
        var flightInfo = result.First();
        Assert.Equal(expectedFlightInfo.Descriptor.Command, flightInfo.Descriptor.Command);
        Assert.Equal(expectedFlightInfo.Descriptor.Type, flightInfo.Descriptor.Type);
        Assert.Equal(expectedFlightInfo.Schema.FieldsList.Count, flightInfo.Schema.FieldsList.Count);
        for (int i = 0; i < expectedFlightInfo.Schema.FieldsList.Count; i++)
        {
            var expectedField = expectedFlightInfo.Schema.FieldsList[i];
            var actualField = flightInfo.Schema.FieldsList[i];
            Assert.Equal(expectedField.Name, actualField.Name);
            Assert.Equal(expectedField.DataType, actualField.DataType);
            Assert.Equal(expectedField.IsNullable, actualField.IsNullable);
        }

        Assert.Equal(expectedFlightInfo.Endpoints.Count, flightInfo.Endpoints.Count);
    }

    [Fact]
    public async Task GetCatalogsSchemaAsync()
    {
        // Arrange
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var schema = await _flightSqlClient.GetCatalogsSchemaAsync();

        // Assert
        Assert.NotNull(schema);
        var expectedFlightInfo = flightHolder.GetFlightInfo();
        for (int i = 0; i < expectedFlightInfo.Schema.FieldsList.Count; i++)
        {
            var expectedField = expectedFlightInfo.Schema.FieldsList[i];
            var actualField = schema.FieldsList[i];
            Assert.Equal(expectedField.Name, actualField.Name);
            Assert.Equal(expectedField.DataType, actualField.DataType);
            Assert.Equal(expectedField.IsNullable, actualField.IsNullable);
        }
    }

    [Fact]
    public async Task GetDbSchemasSchemaAsync()
    {
        // Arrange
        var options = new FlightCallOptions();
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var schema = await _flightSqlClient.GetDbSchemasSchemaAsync(options);

        // Assert
        Assert.NotNull(schema);
        for (int i = 0; i < schema.FieldsList.Count; i++)
        {
            var expectedField = schema.FieldsList[i];
            var actualField = schema.FieldsList[i];
            Assert.Equal(expectedField.Name, actualField.Name);
            Assert.Equal(expectedField.DataType, actualField.DataType);
            Assert.Equal(expectedField.IsNullable, actualField.IsNullable);
        }
    }

    [Fact]
    public async Task DoPutAsync()
    {
        // Arrange
        var schema = new Schema.Builder()
            .Field(f => f.Name("DATA_TYPE_ID").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("TYPE_NAME").DataType(StringType.Default).Nullable(false))
            .Field(f => f.Name("PRECISION").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("LITERAL_PREFIX").DataType(StringType.Default).Nullable(false))
            .Field(f => f.Name("COLUMN_SIZE").DataType(Int32Type.Default).Nullable(false))
            .Build();
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");

        int[] dataTypeIds = { 1, 2, 3 };
        string[] typeNames = ["INTEGER", "VARCHAR", "BOOLEAN"];
        int[] precisions = { 32, 255, 1 };
        string[] literalPrefixes = ["N'", "'", "b'"];
        int[] columnSizes = [10, 255, 1];

        var recordBatch = new RecordBatch(schema,
        [
            new Int32Array.Builder().AppendRange(dataTypeIds).Build(),
            new StringArray.Builder().AppendRange(typeNames).Build(),
            new Int32Array.Builder().AppendRange(precisions).Build(),
            new StringArray.Builder().AppendRange(literalPrefixes).Build(),
            new Int32Array.Builder().AppendRange(columnSizes).Build()
        ], 5);
        Assert.NotNull(recordBatch);
        Assert.Equal(5, recordBatch.Length);
        var flightHolder = new FlightHolder(flightDescriptor, schema, _testWebFactory.GetAddress());
        flightHolder.AddBatch(new RecordBatchWithMetadata(_testUtils.CreateTestBatch(0, 100)));
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        var expectedBatch = _testUtils.CreateTestBatch(0, 100);

        // Act
        var result = await _flightSqlClient.DoPutAsync(flightDescriptor, expectedBatch);

        // Assert
        Assert.NotNull(result);
    }

    [Fact]
    public async Task GetExportedKeysAsync()
    {
        // Arrange
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var tableRef = new TableRef("test-catalog", "test-schema", "test-table");
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var flightInfo = await _flightSqlClient.GetExportedKeysAsync(tableRef);

        // Assert
        Assert.NotNull(flightInfo);
        Assert.Equal("test", flightInfo.Descriptor.Command.ToStringUtf8());
    }

    [Fact]
    public async Task GetExportedKeysSchemaAsync()
    {
        // Arrange
        var options = new FlightCallOptions();
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var tableRef = new TableRef("test-catalog", "test-schema", "test-table");
        var schema = await _flightSqlClient.GetExportedKeysSchemaAsync(tableRef);

        // Assert
        Assert.NotNull(schema);
        Assert.True(schema.FieldsList.Count > 0, "Schema should contain fields.");
        Assert.Equal("test", schema.FieldsList.First().Name);
    }

    [Fact]
    public async Task GetImportedKeysAsync()
    {
        // Arrange
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var flightInfo =
            await _flightSqlClient.GetImportedKeysAsync(new TableRef("test-catalog", "test-schema", "test-table"));

        // Assert
        Assert.NotNull(flightInfo);
        for (int i = 0; i < recordBatch.Schema.FieldsList.Count; i++)
        {
            var expectedField = recordBatch.Schema.FieldsList[i];
            var actualField = flightInfo.Schema.FieldsList[i];
            Assert.Equal(expectedField.Name, actualField.Name);
            Assert.Equal(expectedField.DataType, actualField.DataType);
            Assert.Equal(expectedField.IsNullable, actualField.IsNullable);
        }
    }

    [Fact]
    public async Task GetImportedKeysSchemaAsync()
    {
        // Arrange
        var options = new FlightCallOptions();
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var schema = await _flightSqlClient.GetImportedKeysSchemaAsync(options);

        // Assert
        var expectedSchema = recordBatch.Schema;
        Assert.NotNull(schema);
        Assert.Equal(expectedSchema.FieldsList.Count, schema.FieldsList.Count);
        CompareSchemas(expectedSchema, schema);
    }

    [Fact]
    public async Task GetCrossReferenceAsync()
    {
        // Arrange
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema, _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);
        var pkTableRef = new TableRef("PKCatalog", "PKSchema", "PKTable");
        var fkTableRef = new TableRef("FKCatalog", "FKSchema", "FKTable");

        // Act
        var flightInfo = await _flightSqlClient.GetCrossReferenceAsync(pkTableRef, fkTableRef);

        // Assert
        Assert.NotNull(flightInfo);
        Assert.Equal(flightDescriptor, flightInfo.Descriptor);
        Assert.Single(flightInfo.Schema.FieldsList);
    }

    [Fact]
    public async Task GetCrossReferenceSchemaAsync()
    {
        // Arrange
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var schema = await _flightSqlClient.GetCrossReferenceSchemaAsync();

        // Assert
        var expectedSchema = recordBatch.Schema;
        Assert.NotNull(schema);
        Assert.Equal(expectedSchema.FieldsList.Count, schema.FieldsList.Count);
    }

    [Fact]
    public async Task GetTableTypesAsync()
    {
        // Arrange
        var expectedSchema = new Schema.Builder()
            .Field(f => f.Name("DATA_TYPE_ID").DataType(Int32Type.Default).Nullable(false))
            .Build();
        var commandGetTableTypes = new CommandGetTableTypes();
        byte[] packedCommand = commandGetTableTypes.PackAndSerialize().ToByteArray();
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor(packedCommand);
        var flightHolder = new FlightHolder(flightDescriptor, expectedSchema, "http://localhost:5000");
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var flightInfo = await _flightSqlClient.GetTableTypesAsync();
        var actualSchema = flightInfo.Schema;

        // Assert
        Assert.NotNull(flightInfo);
        CompareSchemas(expectedSchema, actualSchema);
    }

    [Fact]
    public async Task GetTableTypesSchemaAsync()
    {
        // Arrange
        var options = new FlightCallOptions();
        var expectedSchema = new Schema.Builder()
            .Field(f => f.Name("DATA_TYPE_ID").DataType(Int32Type.Default).Nullable(false))
            .Build();
        var commandGetTableTypesSchema = new CommandGetTableTypes();
        byte[] packedCommand = commandGetTableTypesSchema.PackAndSerialize().ToByteArray();
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor(packedCommand);

        var flightHolder = new FlightHolder(flightDescriptor, expectedSchema, "http://localhost:5000");
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var schemaResult = await _flightSqlClient.GetTableTypesSchemaAsync(options);

        // Assert
        Assert.NotNull(schemaResult);
        CompareSchemas(expectedSchema, schemaResult);
    }

    [Fact]
    public async Task GetXdbcTypeInfoAsync()
    {
        // Arrange
        var options = new FlightCallOptions();
        var expectedSchema = new Schema.Builder()
            .Field(f => f.Name("DATA_TYPE_ID").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("TYPE_NAME").DataType(StringType.Default).Nullable(false))
            .Field(f => f.Name("PRECISION").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("LITERAL_PREFIX").DataType(StringType.Default).Nullable(false))
            .Field(f => f.Name("COLUMN_SIZE").DataType(Int32Type.Default).Nullable(false))
            .Build();
        var commandGetXdbcTypeInfo = new CommandGetXdbcTypeInfo();
        byte[] packedCommand = commandGetXdbcTypeInfo.PackAndSerialize().ToByteArray();
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor(packedCommand);

        // Creating a flight holder with the expected schema and adding it to the flight store
        var flightHolder = new FlightHolder(flightDescriptor, expectedSchema, "http://localhost:5000");
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var flightInfo = await _flightSqlClient.GetXdbcTypeInfoAsync(options);

        // Assert
        Assert.NotNull(flightInfo);
        CompareSchemas(expectedSchema, flightInfo.Schema);
    }

    [Fact]
    public async Task GetXdbcTypeInfoSchemaAsync()
    {
        // Arrange
        var options = new FlightCallOptions();
        var expectedSchema = new Schema.Builder()
            .Field(f => f.Name("DATA_TYPE_ID").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("TYPE_NAME").DataType(StringType.Default).Nullable(false))
            .Field(f => f.Name("PRECISION").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("LITERAL_PREFIX").DataType(StringType.Default).Nullable(false))
            .Build();

        var commandGetXdbcTypeInfo = new CommandGetXdbcTypeInfo();
        byte[] packedCommand = commandGetXdbcTypeInfo.PackAndSerialize().ToByteArray();
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor(packedCommand);

        var flightHolder = new FlightHolder(flightDescriptor, expectedSchema, "http://localhost:5000");
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var schema = await _flightSqlClient.GetXdbcTypeInfoSchemaAsync(options);

        // Assert
        Assert.NotNull(schema);
        CompareSchemas(expectedSchema, schema);
    }

    [Fact]
    public async Task GetSqlInfoSchemaAsync()
    {
        // Arrange
        var options = new FlightCallOptions();
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("sqlInfo");
        var expectedSchema = new Schema.Builder()
            .Field(f => f.Name("DATA_TYPE_ID").DataType(Int32Type.Default).Nullable(false))
            .Build();
        var flightHolder = new FlightHolder(flightDescriptor, expectedSchema, _testWebFactory.GetAddress());
        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        var schema = await _flightSqlClient.GetSqlInfoSchemaAsync(options);

        // Assert
        Assert.NotNull(schema);
        CompareSchemas(expectedSchema, schema);
    }

    [Fact]
    public async Task CancelFlightInfoAsync()
    {
        // Arrange
        var schema = new Schema.Builder()
            .Field(f => f.Name("DATA_TYPE_ID").DataType(Int32Type.Default).Nullable(false))
            .Build();
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var flightInfo = new FlightInfo(schema, flightDescriptor, new List<FlightEndpoint>(), 0, 0);
        var cancelRequest = new FlightInfoCancelRequest(flightInfo);

        // Act
        var cancelResult = await _flightSqlClient.CancelFlightInfoAsync(cancelRequest);

        // Assert
        Assert.Equal(1, cancelResult.GetCancelStatus());
    }

    [Fact]
    public async Task CancelQueryAsync()
    {
        // Arrange
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var schema = new Schema.Builder()
            .Field(f => f.Name("DATA_TYPE_ID").DataType(Int32Type.Default).Nullable(false))
            .Build();
        var flightInfo = new FlightInfo(schema, flightDescriptor, new List<FlightEndpoint>(), 0, 0);

        // Adding the flight info to the flight store for testing
        _flightStore.Flights.Add(flightDescriptor,
            new FlightHolder(flightDescriptor, schema, _testWebFactory.GetAddress()));

        // Act
        var cancelStatus = await _flightSqlClient.CancelQueryAsync(flightInfo);

        // Assert
        Assert.Equal(1, cancelStatus.GetCancelStatus());
    }

    public void Dispose() => _testWebFactory?.Dispose();

    private void CompareSchemas(Schema expectedSchema, Schema actualSchema)
    {
        Assert.Equal(expectedSchema.FieldsList.Count, actualSchema.FieldsList.Count);

        for (int i = 0; i < expectedSchema.FieldsList.Count; i++)
        {
            var expectedField = expectedSchema.FieldsList[i];
            var actualField = actualSchema.FieldsList[i];

            Assert.Equal(expectedField.Name, actualField.Name);
            Assert.Equal(expectedField.DataType, actualField.DataType);
            Assert.Equal(expectedField.IsNullable, actualField.IsNullable);
            Assert.Equal(expectedField.Metadata, actualField.Metadata);
        }
    }
}
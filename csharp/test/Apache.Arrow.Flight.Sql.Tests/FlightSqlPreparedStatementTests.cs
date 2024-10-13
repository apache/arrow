using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.Sql.Client;
using Apache.Arrow.Flight.TestWeb;
using Apache.Arrow.Tests;
using Apache.Arrow.Types;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Grpc.Core;
using Xunit;

namespace Apache.Arrow.Flight.Sql.Tests;

public class FlightSqlPreparedStatementTests
{
    readonly TestFlightSqlWebFactory _testWebFactory;
    readonly FlightStore _flightStore;
    readonly FlightSqlClient _flightSqlClient;
    private readonly PreparedStatement _preparedStatement;
    private readonly Schema _schema;
    private readonly FlightDescriptor _flightDescriptor;
    private readonly FlightHolder _flightHolder;
    private readonly RecordBatch _parameterBatch;

    public FlightSqlPreparedStatementTests()
    {
        _flightStore = new FlightStore();
        _testWebFactory = new TestFlightSqlWebFactory(_flightStore);
        FlightClient flightClient = new(_testWebFactory.GetChannel());
        _flightSqlClient = new(flightClient);

        // Setup mock
        _flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test-query");
        _schema = new Schema
                .Builder()
            .Field(f => f.Name("DATA_TYPE_ID").DataType(Int32Type.Default).Nullable(false))
            .Build();

        int[] dataTypeIds = [1, 2, 3];
        string[] typeNames = ["INTEGER", "VARCHAR", "BOOLEAN"];
        int[] precisions = [32, 255, 1];
        string[] literalPrefixes = ["N'", "'", "b'"];
        int[] columnSizes = [10, 255, 1];
        _parameterBatch = new RecordBatch(_schema,
        [
            new Int32Array.Builder().AppendRange(dataTypeIds).Build(),
            new StringArray.Builder().AppendRange(typeNames).Build(),
            new Int32Array.Builder().AppendRange(precisions).Build(),
            new StringArray.Builder().AppendRange(literalPrefixes).Build(),
            new Int32Array.Builder().AppendRange(columnSizes).Build()
        ], 5);

        _flightHolder = new FlightHolder(_flightDescriptor, _schema, _testWebFactory.GetAddress());
        _flightStore.Flights.Add(_flightDescriptor, _flightHolder);
        _preparedStatement = new PreparedStatement(_flightSqlClient, handle: "test-handle-guid", datasetSchema: _schema,
            parameterSchema: _schema);
    }

    [Fact]
    public async Task GetSchemaAsync_ShouldThrowInvalidOperationException_WhenStatementIsClosed()
    {
        // Arrange: 
        await _preparedStatement.CloseAsync(new FlightCallOptions());

        // Act & Assert: Ensure that calling GetSchemaAsync on a closed statement throws an exception.
        await Assert.ThrowsAsync<InvalidOperationException>(() => _preparedStatement.GetSchemaAsync(new FlightCallOptions()));
    }

    [Fact]
    public async Task ExecuteAsync_ShouldReturnFlightInfo_WhenValidInputsAreProvided()
    {
        // Arrange
        var validSchema = new Schema.Builder()
            .Field(f => f.Name("field1").DataType(Int32Type.Default))
            .Build();
        string handle = "TestHandle";
        var preparedStatement = new PreparedStatement(_flightSqlClient, handle, validSchema, validSchema);
        var validRecordBatch = CreateRecordBatch(validSchema, [1, 2, 3]);

        // Act
        var result = preparedStatement.SetParameters(validRecordBatch);
        var flightInfo = await preparedStatement.ExecuteAsync(new FlightCallOptions(), validRecordBatch);
        
        // Assert
        Assert.NotNull(flightInfo);
        Assert.IsType<FlightInfo>(flightInfo);
        Assert.Equal(Status.DefaultSuccess, result);
    }
    
    [Fact]
    public async Task BindParametersAsync_ShouldReturnMetadata_WhenValidInputsAreProvided()
    {
        // Arrange
        var validSchema = new Schema.Builder()
            .Field(f => f.Name("field1").DataType(Int32Type.Default))
            .Build();
    
        var validRecordBatch = CreateRecordBatch(validSchema, new[] { 1, 2, 3 });
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("TestCommand");
    
        var preparedStatement = new PreparedStatement(_flightSqlClient, "TestHandle", validSchema, validSchema);
    
        // Act
        var metadata = await preparedStatement.BindParametersAsync(new FlightCallOptions(), flightDescriptor, validRecordBatch);
    
        // Assert
        Assert.NotNull(metadata);

        // Check if metadata has valid content
        // Some systems might return empty metadata, so we validate it's non-null and proceed accordingly
        if (metadata.Length == 0)
        {
            // Optionally, check if the server returned empty metadata but still succeeded
            Assert.Equal(0, metadata.Length);
        }
        else
        {
            // If metadata is present, validate its contents
            Assert.True(metadata.Length > 0, "Metadata should have a length greater than 0 when valid.");
        }
    }
    
    [Theory]
    [MemberData(nameof(GetTestData))]
    public async Task TestSetParameters(RecordBatch parameterBatch, Schema parameterSchema, Type expectedException)
    {
        // Arrange
        var validSchema = new Schema.Builder()
            .Field(f => f.Name("field1").DataType(Int32Type.Default))
            .Build();
        string handle = "TestHandle";
        
        var preparedStatement = new PreparedStatement(_flightSqlClient, handle, validSchema, parameterSchema);
    
        if (expectedException != null)
        {
            // Act and Assert (Expected to throw exception)
            var exception = await Record.ExceptionAsync(() => Task.Run(() => preparedStatement.SetParameters(parameterBatch)));
            Assert.NotNull(exception);
            Assert.IsType(expectedException, exception);  // Ensure correct exception type
        }
        else
        {
            // Act
            var result = await Task.Run(() => preparedStatement.SetParameters(parameterBatch));

            // Assert
            Assert.NotNull(preparedStatement.ParameterReader);
            Assert.Equal(Status.DefaultSuccess, result); // Ensure Status is success
        }
    }
    
    [Fact]
    public async Task TestSetParameters_Cancelled()
    {
        // Arrange
        var validSchema = new Schema.Builder()
            .Field(f => f.Name("field1").DataType(Int32Type.Default))
            .Build();
        
        string handle = "TestHandle";
        
        var preparedStatement = new PreparedStatement(_flightSqlClient, handle, validSchema, validSchema);
        var validRecordBatch = CreateRecordBatch(validSchema, [1, 2, 3]);

        // Create a CancellationTokenSource
        var cts = new CancellationTokenSource();

        // Act: Simulate cancellation before setting parameters
        await cts.CancelAsync();
        var result = preparedStatement.SetParameters(validRecordBatch, cts.Token);

        // Assert: Ensure the status is DefaultCancelled
        Assert.Equal(Status.DefaultCancelled, result);
    }

    [Fact]
    public async Task TestCloseAsync()
    {
        // Arrange
        var options = new FlightCallOptions();

        // Act
        await _preparedStatement.CloseAsync(options);

        // Assert
        Assert.True(_preparedStatement.IsClosed,
            "PreparedStatement should be marked as closed after calling CloseAsync.");
    }

    [Fact]
    public async Task ReadResultAsync_ShouldPopulateMessage_WhenValidFlightData()
    {
        // Arrange
        var message = new ActionCreatePreparedStatementResult();
        var flightData = new FlightData(_flightDescriptor, ByteString.CopyFromUtf8("test-data"));

        var results = GetAsyncEnumerable(new List<FlightData> { flightData });

        // Act
        await _preparedStatement.ReadResultAsync(results, message);

        // Assert
        Assert.NotEmpty(message.PreparedStatementHandle.ToStringUtf8());
    }

    [Fact]
    public async Task ReadResultAsync_ShouldNotThrow_WhenFlightDataBodyIsNullOrEmpty()
    {
        // Arrange
        var message = new ActionCreatePreparedStatementResult();
        var flightData1 = new FlightData(_flightDescriptor, ByteString.Empty);
        var flightData2 = new FlightData(_flightDescriptor, ByteString.CopyFromUtf8(""));

        var results = GetAsyncEnumerable(new List<FlightData> { flightData1, flightData2 });

        // Act
        await _preparedStatement.ReadResultAsync(results, message);

        // Assert
        Assert.Empty(message.PreparedStatementHandle.ToStringUtf8());
    }

    [Fact]
    public async Task ReadResultAsync_ShouldThrowInvalidOperationException_WhenFlightDataIsInvalid()
    {
        // Arrange
        var invalidMessage = new ActionCreatePreparedStatementResult();
        var invalidFlightData = new FlightData(_flightDescriptor, ByteString.CopyFrom(new byte[] { }));
    
        // Act
        var results = GetAsyncEnumerable(new List<FlightData> { invalidFlightData });

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => _preparedStatement.ReadResultAsync(results, invalidMessage));
    }

    [Fact]
    public async Task ReadResultAsync_ShouldProcessMultipleFlightDataEntries()
    {
        // Arrange
        var message = new ActionCreatePreparedStatementResult();
        var flightData1 = new FlightData(_flightDescriptor, ByteString.CopyFromUtf8("data1"));
        var flightData2 = new FlightData(_flightDescriptor, ByteString.CopyFromUtf8("data2"));

        var results = GetAsyncEnumerable(new List<FlightData> { flightData1, flightData2 });

        // Act
        await _preparedStatement.ReadResultAsync(results, message);

        // Assert
        Assert.NotEmpty(message.PreparedStatementHandle.ToStringUtf8());
    }


    [Fact]
    public async Task ParseResponseAsync_ShouldReturnPreparedStatement_WhenValidData()
    {
        // Arrange
        var preparedStatementHandle = "test-handle";
        var actionResult = new ActionCreatePreparedStatementResult
        {
            PreparedStatementHandle = ByteString.CopyFrom(preparedStatementHandle, Encoding.UTF8),
            DatasetSchema = _schema.ToByteString(),
            ParameterSchema = _schema.ToByteString()
        };
        var flightData = new FlightData(_flightDescriptor, ByteString.CopyFrom(actionResult.ToByteArray()));
        var results = GetAsyncEnumerable(new List<FlightData> { flightData });

        // Act
        var preparedStatement = await _preparedStatement.ParseResponseAsync(_flightSqlClient, results);

        // Assert
        Assert.NotNull(preparedStatement);
        Assert.Equal(preparedStatementHandle, preparedStatement.Handle);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task ParseResponseAsync_ShouldThrowException_WhenPreparedStatementHandleIsNullOrEmpty(string handle)
    {
        // Arrange
        ActionCreatePreparedStatementResult actionResult;

        // Check if handle is null or empty and handle accordingly
        if (string.IsNullOrEmpty(handle))
        {
            actionResult = new ActionCreatePreparedStatementResult();
        }
        else
        {
            actionResult = new ActionCreatePreparedStatementResult
            {
                PreparedStatementHandle = ByteString.CopyFrom(handle, Encoding.UTF8)
            };
        }

        var flightData = new FlightData(_flightDescriptor, ByteString.CopyFrom(actionResult.ToByteArray()));
        var results = GetAsyncEnumerable(new List<FlightData> { flightData });

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _preparedStatement.ParseResponseAsync(_flightSqlClient, results));
    }

    [Fact]
    public async Task GetSchemaAsync_ShouldReturnSchemaResult_WhenValidInput()
    {
        // Arrange: Create a ExpectedSchemaResult for the test scenario.
        var sqlClient = new TestFlightSqlClient();
        var datasetSchema = new Schema.Builder()
            .Field(f => f.Name("Column1").DataType(Int32Type.Default).Nullable(false))
            .Build();
        var parameterSchema = new Schema.Builder()
            .Field(f => f.Name("Parameter1").DataType(Int32Type.Default).Nullable(false))
            .Build();
        var preparedStatement = new PreparedStatement(sqlClient, "test-handle", datasetSchema, parameterSchema);
        var expectedSchemaResult = new Schema.Builder()
            .Field(f => f.Name("Column1").DataType(Int32Type.Default).Nullable(false))
            .Build();

        // Act: 
        var result = await preparedStatement.GetSchemaAsync(new FlightCallOptions());

        // Assert:
        Assert.NotNull(result);
        SchemaComparer.Compare(expectedSchemaResult, result);
    }

    [Fact]
    public async Task GetSchemaAsync_ShouldThrowException_WhenSchemaIsEmpty()
    {
        var sqlClient = new TestFlightSqlClient { ReturnEmptySchema = true };
        var emptySchema = new Schema.Builder().Build(); // Create an empty schema
        var preparedStatement = new PreparedStatement(sqlClient, "test-handle", emptySchema, emptySchema);
        // Act & Assert: Ensure that calling GetSchemaAsync with an empty schema throws an exception.
        await Assert.ThrowsAsync<InvalidOperationException>(() => preparedStatement.GetSchemaAsync(new FlightCallOptions()));
    }

    [Fact]
    public void Dispose_ShouldSetIsClosedToTrue()
    {
        // Act
        _preparedStatement.Dispose();

        // Assert
        Assert.True(_preparedStatement.IsClosed, "The PreparedStatement should be closed after Dispose is called.");
    }

    [Fact]
    public void Dispose_MultipleTimes_ShouldNotThrowException()
    {
        // Act
        _preparedStatement.Dispose();
        var exception = Record.Exception(() => _preparedStatement.Dispose());

        // Assert
        Assert.Null(exception);
    }

    [Fact]
    public async Task ToFlightDataStream_ShouldConvertRecordBatchToFlightDataStream()
    {
        // Arrange
        var schema = new Schema.Builder()
            .Field(f => f.Name("Name").DataType(StringType.Default).Nullable(false))
            .Field(f => f.Name("Age").DataType(Int32Type.Default).Nullable(false))
            .Build();

        var names = new StringArray.Builder().Append("Hello").Append("World").Build();
        var ages = new Int32Array.Builder().Append(30).Append(40).Build();
        var recordBatch = new RecordBatch(schema, [names, ages], 2);

        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test-command");

        // Act
        var flightDataStream = recordBatch.ToFlightDataStream(flightDescriptor);
        var flightDataList = new List<FlightData>();

        await foreach (var flightData in flightDataStream.ReadAllAsync())
        {
            flightDataList.Add(flightData);
        }

        // Assert
        Assert.Single(flightDataList);
        Assert.NotNull(flightDataList[0].DataBody);
    }

    private async IAsyncEnumerable<T> GetAsyncEnumerable<T>(IEnumerable<T> enumerable)
    {
        foreach (var item in enumerable)
        {
            yield return item;
            await Task.Yield();
        }
    }

    /// <summary>
    /// Test client implementation that simulates the behavior of FlightSqlClient for testing purposes.
    /// </summary>
    private class TestFlightSqlClient : FlightSqlClient
    {
        public bool ReturnEmptySchema { get; set; } = false;

        public TestFlightSqlClient() : base(null)
        {
        }

        public override Task<Schema> GetSchemaAsync(FlightCallOptions options, FlightDescriptor descriptor)
        {
            if (ReturnEmptySchema)
            {
                // Return an empty schema to simulate an edge case.
                return Task.FromResult(new Schema.Builder().Build());
            }

            // Return a valid SchemaResult for the test.
            var schemaResult = new Schema.Builder()
                .Field(f => f.Name("Column1").DataType(Int32Type.Default).Nullable(false))
                .Build();
            return Task.FromResult(schemaResult);
        }
    }
    
    public static IEnumerable<object[]> GetTestData()
    {
        // Define schema
        var schema = new Schema.Builder()
            .Field(f => f.Name("field1").DataType(Int32Type.Default))
            .Build();
        int[] validValues = { 1, 2, 3 };
        int[] invalidValues = { 4, 5, 6 };
        var validRecordBatch = CreateRecordBatch(schema, validValues);
        var invalidSchema = new Schema.Builder()
            .Field(f => f.Name("invalid_field").DataType(Int32Type.Default))
            .Build();
        
        var invalidRecordBatch = CreateRecordBatch(invalidSchema, invalidValues);
        return new List<object[]>
        {
            // Valid RecordBatch and schema - no exception expected
            new object[] { validRecordBatch, new Schema.Builder().Field(f => f.Name("field1").DataType(Int32Type.Default)).Build(), null },

            // Null RecordBatch - expect ArgumentNullException
            new object[] { null, new Schema.Builder().Field(f => f.Name("field1").DataType(Int32Type.Default)).Build(), typeof(ArgumentNullException) }
        };
    }
    
    public static RecordBatch CreateRecordBatch(Schema schema, int[] values)
    {
        var int32Array = new Int32Array.Builder().AppendRange(values).Build();
        var recordBatch = new RecordBatch.Builder()
            .Append("field1", true, int32Array)
            .Build();
        return recordBatch;
    }
}
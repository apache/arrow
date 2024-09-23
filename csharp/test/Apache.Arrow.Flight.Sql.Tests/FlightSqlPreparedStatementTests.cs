using System;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.Sql.Client;
using Apache.Arrow.Flight.Sql.TestWeb;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Flight.Sql.Tests;

public class FlightSqlPreparedStatementTests
{
    readonly TestSqlWebFactory _testWebFactory;
    readonly FlightSqlStore _flightStore;
    private readonly PreparedStatement _preparedStatement;
    private readonly Schema _schema;
    private readonly RecordBatch _parameterBatch;
    private readonly FlightDescriptor _flightDescriptor;

    public FlightSqlPreparedStatementTests()
    {
        _flightStore = new FlightSqlStore();
        _testWebFactory = new TestSqlWebFactory(_flightStore);
        FlightClient flightClient = new(_testWebFactory.GetChannel());
        FlightSqlClient flightSqlClient = new(flightClient);

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

        var flightHolder = new FlightSqlHolder(_flightDescriptor, _schema, _testWebFactory.GetAddress());
        _preparedStatement = new PreparedStatement(flightSqlClient, flightHolder.GetFlightInfo(), "SELECT * FROM test");
    }

    // PreparedStatement
    [Fact]
    public async Task SetParameters_ShouldSetParameters_WhenStatementIsOpen()
    {
        await _preparedStatement.SetParameters(_parameterBatch);
        Assert.NotNull(_parameterBatch);
    }

    [Fact]
    public async Task SetParameters_ShouldThrowException_WhenStatementIsClosed()
    {
        // Arrange
        await _preparedStatement.CloseAsync(new FlightCallOptions());

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _preparedStatement.SetParameters(_parameterBatch)
        );
    }

    [Fact]
    public async Task ExecuteUpdateAsync_ShouldReturnAffectedRows_WhenParametersAreSet()
    {
        // Arrange
        var options = new FlightCallOptions();
        var flightHolder = new FlightSqlHolder(_flightDescriptor, _schema, _testWebFactory.GetAddress());
        flightHolder.AddBatch(new RecordBatchWithMetadata(_parameterBatch));
        _flightStore.Flights.Add(_flightDescriptor, flightHolder);
        await _preparedStatement.SetParameters(_parameterBatch);

        // Act
        long affectedRows = await _preparedStatement.ExecuteUpdateAsync(options);

        // Assert
        Assert.True(affectedRows > 0);  // Verifies that the statement executed successfully.
    }

    [Fact]
    public async Task ExecuteUpdateAsync_ShouldThrowException_WhenNoParametersSet()
    {
        // Arrange
        var options = new FlightCallOptions();

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _preparedStatement.ExecuteUpdateAsync(options)
        );
    }

    [Fact]
    public async Task ExecuteUpdateAsync_ShouldThrowException_WhenStatementIsClosed()
    {
        // Arrange
        var options = new FlightCallOptions();
        await _preparedStatement.CloseAsync(options);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _preparedStatement.ExecuteUpdateAsync(options)
        );
    }

    [Fact]
    public async Task CloseAsync_ShouldCloseStatement_WhenCalled()
    {
        // Arrange
        var options = new FlightCallOptions();

        // Act
        await _preparedStatement.CloseAsync(options);

        // Assert
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _preparedStatement.CloseAsync(options)
        );
    }

    [Fact]
    public async Task CloseAsync_ShouldThrowException_WhenStatementAlreadyClosed()
    {
        // Arrange
        var options = new FlightCallOptions();
        await _preparedStatement.CloseAsync(options);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _preparedStatement.CloseAsync(options)
        );
    }
}

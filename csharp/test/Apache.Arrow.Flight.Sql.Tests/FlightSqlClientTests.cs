using System;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.Sql.Client;
using Apache.Arrow.Flight.Sql.TestWeb;
using Grpc.Core.Utils;
using Xunit;

namespace Apache.Arrow.Flight.Sql.Tests;

public class FlightSqlClientTests : IDisposable
{
    readonly TestSqlWebFactory _testWebFactory;
    readonly FlightSqlStore _flightStore;
    readonly FlightClient _flightClient;
    private readonly FlightSqlClient _flightSqlClient;
    private readonly FlightSqlTestUtils _testUtils;

    public FlightSqlClientTests()
    {
        _flightStore = new FlightSqlStore();
        _testWebFactory = new TestSqlWebFactory(_flightStore);
        _flightClient = new FlightClient(_testWebFactory.GetChannel());
        _flightSqlClient = new FlightSqlClient(_flightClient);

        _testUtils = new FlightSqlTestUtils(_testWebFactory, _flightStore);
    }

    #region Transactions

    [Fact]
    public async Task CommitAsync_Transaction()
    {
        // Arrange
        string transactionId = "sample-transaction-id";
        var options = new FlightCallOptions();
        var transaction = new Transaction(transactionId);

        // Act
        var streamCall = _flightSqlClient.CommitAsync(options, transaction);
        var result = await streamCall.ResponseStream.ToListAsync();

        // Assert
        Assert.NotNull(result);
        Assert.Equal(transaction.TransactionId, result.FirstOrDefault()?.Body.ToStringUtf8());
    }

    [Fact]
    public async Task BeginTransactionAsync_Transaction()
    {
        // Arrange
        var options = new FlightCallOptions();
        string expectedTransactionId = "sample-transaction-id";

        // Act
        var transaction = await _flightSqlClient.BeginTransactionAsync(options);

        // Assert
        Assert.NotNull(transaction);
        Assert.Equal(expectedTransactionId, transaction.TransactionId);
    }

    [Fact]
    public async Task RollbackAsync_Transaction()
    {
        // Arrange
        string transactionId = "sample-transaction-id";
        var options = new FlightCallOptions();
        var transaction = new Transaction(transactionId);

        // Act
        var streamCall = _flightSqlClient.RollbackAsync(options, transaction);
        var result = await streamCall.ResponseStream.ToListAsync();

        // Assert
        Assert.NotNull(transaction);
        Assert.Equal(result.FirstOrDefault()?.Body.ToStringUtf8(), transaction.TransactionId);
    }

    #endregion

    #region PreparedStatement

    [Fact]
    public async Task PreparedStatement()
    {
        // Arrange
        string query = "INSERT INTO users (id, name) VALUES (1, 'John Doe')";
        var options = new FlightCallOptions();

        // Act
        var preparedStatement = await _flightSqlClient.PrepareAsync(options, query);

        // Assert
        Assert.NotNull(preparedStatement);
    }

    #endregion

    [Fact]
    public async Task ExecuteUpdateAsync_ShouldReturnAffectedRows()
    {
        // Arrange
        string query = "UPDATE test_table SET column1 = 'value' WHERE column2 = 'condition'";
        var options = new FlightCallOptions();
        var transaction = new Transaction("sample-transaction-id");
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);

        var flightHolder = new FlightSqlHolder(flightDescriptor, recordBatch.Schema, _testWebFactory.GetAddress());
        flightHolder.AddBatch(new RecordBatchWithMetadata(recordBatch));

        _flightStore.Flights.Add(flightDescriptor, flightHolder);

        // Act
        long affectedRows = await _flightSqlClient.ExecuteUpdateAsync(options, query, transaction);

        // Assert
        Assert.Equal(100, affectedRows);
    }

    [Fact]
    public async Task Execute()
    {
        // Arrange
        string query = "SELECT * FROM test_table";
        var options = new FlightCallOptions();

        // Act
        var flightInfo = await _flightSqlClient.ExecuteAsync(options, query);

        // Assert
        Assert.NotNull(flightInfo);
        Assert.Single(flightInfo.Endpoints);
    }

    [Fact]
    public async Task GetFlightInfo()
    {
        // Arrange
        var options = new FlightCallOptions();
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor("test");
        var recordBatch = _testUtils.CreateTestBatch(0, 100);
        var flightHolder = new FlightSqlHolder(flightDescriptor, recordBatch.Schema,
            _testWebFactory.GetAddress());

        _flightStore.Flights.Add(flightDescriptor, flightHolder);
        // Act
        var flightInfo = await _flightSqlClient.GetFlightInfoAsync(options, flightDescriptor);

        // Assert
        Assert.NotNull(flightInfo);
    }

    public void Dispose() => _testWebFactory?.Dispose();
}

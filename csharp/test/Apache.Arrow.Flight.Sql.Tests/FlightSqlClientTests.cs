using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.Sql.Client;
using Apache.Arrow.Flight.Tests;
using Apache.Arrow.Flight.TestWeb;
using Google.Protobuf;
using Grpc.Core;
using Xunit;

namespace Apache.Arrow.Flight.Sql.Tests;

public class FlightSqlClientTests : IDisposable
{
    readonly TestWebFactory _testWebFactory;
    readonly FlightStore _flightStore;
    readonly FlightClient _flightClient;

    public FlightSqlClientTests()
    {
        _flightStore = new FlightStore();
        _testWebFactory = new TestWebFactory(_flightStore);
        _flightClient = new FlightClient(_testWebFactory.GetChannel());
    }

    [Fact]
    public void CommitAsync_CommitsTransaction()
    {
        // Arrange
        string transactionId = "sample-transaction-id";
        var client = new FlightSqlClient(_flightClient);
        var options = new FlightCallOptions();
        var transaction = new Transaction(transactionId);

        // Act
        var streamCall = client.CommitAsync(options, transaction);

        // Assert
        // Assert.Contains("Transaction committed successfully.", consoleOutput.ToString());
    }

    public void Dispose() => _testWebFactory?.Dispose();
}

using System.Collections.Generic;
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

public class FlightSqlClientTests
{
    readonly TestWebFactory _testWebFactory;
    readonly FlightStore _flightStore;

    public FlightSqlClientTests()
    {
        _flightStore = new FlightStore();
        _testWebFactory = new TestWebFactory(_flightStore);
    }

    [Fact]
    public async Task CommitAsync_CommitsTransactionSuccessfully()
    {
        // Arrange
        var mockClient = new MockFlightClient(_testWebFactory.GetChannel());
        string transactionId = "sample-transaction-id";
        var expectedResult =
            new FlightResult(ByteString.CopyFromUtf8("Transaction committed successfully."));

        mockClient.SetupActionResult("Commit", [expectedResult]);
        var client = new FlightSqlClient(mockClient);
        var options = new FlightCallOptions();
        var transaction = new Transaction(transactionId);

        // Act
        await client.CommitAsync(options, transaction);

        // Assert
        Assert.Single(mockClient.SentActions);
        Assert.Equal("Commit", mockClient.SentActions.First().Type);
        Assert.Equal(transactionId, mockClient.SentActions.First().Body.ToStringUtf8());
    }

    [Fact]
    public async Task CommitAsync_WithNoActions_ShouldNotCommitTransaction()
    {
        // Arrange
        var mockClient = new MockFlightClient(_testWebFactory.GetChannel(), new NoActionStrategy());
        var client = new FlightSqlClient(mockClient);
        var options = new FlightCallOptions();
        var transaction = new Transaction("sample-transaction-id");

        // Act
        await client.CommitAsync(options, transaction);

        // Assert
        Assert.Empty(mockClient.SentActions);
    }
}

public class MockFlightClient : FlightClient, IFlightClient
{
    private readonly Dictionary<string, List<FlightResult>> _actionResults;
    private readonly List<FlightAction> _sentActions = new();
    private readonly IActionStrategy _actionStrategy;

    public MockFlightClient(ChannelBase channel, IActionStrategy actionStrategy = null) : base(channel)
    {
        _actionResults = new Dictionary<string, List<FlightResult>>();
        _actionStrategy = actionStrategy ?? new DefaultActionStrategy();
    }

    public void SetupActionResult(string actionType, List<FlightResult> results)
    {
        _actionResults[actionType] = results;
    }

    public new AsyncServerStreamingCall<FlightResult> DoAction(FlightAction action, Metadata headers = null)
    {
        _actionStrategy.HandleAction(this, action);

        var result = _actionResults.TryGetValue(action.Type, out List<FlightResult> actionResult)
            ? actionResult
            : new List<FlightResult>();

        var responseStream = new MockAsyncStreamReader<FlightResult>(result);
        return new AsyncServerStreamingCall<FlightResult>(
            responseStream,
            Task.FromResult(new Metadata()),
            () => Status.DefaultSuccess,
            () => new Metadata(),
            () => { });
    }

    public List<FlightAction> SentActions => _sentActions;

    private class MockAsyncStreamReader<T>(IEnumerable<T> items) : IAsyncStreamReader<T>
    {
        private readonly IEnumerator<T> _enumerator = items.GetEnumerator();

        public T Current => _enumerator.Current;


        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            return await Task.Run(() => _enumerator.MoveNext(), cancellationToken);
        }

        public void Dispose()
        {
            _enumerator.Dispose();
        }
    }
}

public interface IActionStrategy
{
    void HandleAction(MockFlightClient mockClient, FlightAction action);
}

internal class DefaultActionStrategy : IActionStrategy
{
    public void HandleAction(MockFlightClient mockClient, FlightAction action)
    {
        mockClient.SentActions.Add(action);
    }
}

internal class NoActionStrategy : IActionStrategy
{
    public void HandleAction(MockFlightClient mockClient, FlightAction action)
    {
        // Do nothing, or handle the action differently
    }
}

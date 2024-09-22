using System;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Sql.Client;
using Arrow.Flight.Protocol.Sql;
using Grpc.Core;

namespace Apache.Arrow.Flight.Sql;

// TODO: Refactor this to match C++ implementation
public class PreparedStatement
{
    private readonly FlightSqlClient _client;
    private readonly FlightInfo _flightInfo;
    private RecordBatch? _parameterBatch;
    private readonly string _query;
    private bool _isClosed;

    public PreparedStatement(FlightSqlClient client, FlightInfo flightInfo, string query)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _flightInfo = flightInfo ?? throw new ArgumentNullException(nameof(flightInfo));
        _query = query ?? throw new ArgumentNullException(nameof(query));
        _isClosed = false;
    }

    /// <summary>
    /// Set parameters for the prepared statement
    /// </summary>
    /// <param name="parameterBatch">The batch of parameters to bind</param>
    public Task SetParameters(RecordBatch parameterBatch)
    {
        if (_isClosed)
        {
            throw new InvalidOperationException("Cannot set parameters on a closed statement.");
        }

        _parameterBatch = parameterBatch ?? throw new ArgumentNullException(nameof(parameterBatch));
        return Task.CompletedTask;
    }

    /// <summary>
    /// Execute the prepared statement, returning the number of affected rows
    /// </summary>
    /// <param name="options">The FlightCallOptions for the execution</param>
    /// <returns>Task representing the asynchronous operation</returns>
    public async Task<long> ExecuteUpdateAsync(FlightCallOptions options)
    {
        if (_isClosed)
        {
            throw new InvalidOperationException("Cannot execute a closed statement.");
        }

        if (_parameterBatch == null)
        {
            throw new InvalidOperationException("No parameters set for the prepared statement.");
        }

        var commandSqlCall = new CommandPreparedStatementQuery
        {
            PreparedStatementHandle = _flightInfo.Endpoints.First().Ticket.Ticket
        };
        byte[] packedCommand = commandSqlCall.PackAndSerialize();
        var descriptor = FlightDescriptor.CreateCommandDescriptor(packedCommand);

        var flightInfo = await _client.GetFlightInfoAsync(options, descriptor);
        return await ExecuteAndGetAffectedRowsAsync(options, flightInfo);
    }

    /// <summary>
    /// Closes the prepared statement
    /// </summary>
    public async Task CloseAsync(FlightCallOptions options)
    {
        if (_isClosed)
        {
            throw new InvalidOperationException("Statement already closed.");
        }

        try
        {
            var actionClose = new FlightAction(SqlAction.CloseRequest, _flightInfo.Descriptor.Command);
            await foreach (var result in _client.DoActionAsync(options, actionClose).ConfigureAwait(false))
            {
                // Process any result if necessary (e.g., logging).
            }
            _isClosed = true;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to close the prepared statement", ex);
        }
    }

    /// <summary>
    /// Helper method to execute the statement and get affected rows
    /// </summary>
    private async Task<long> ExecuteAndGetAffectedRowsAsync(FlightCallOptions options, FlightInfo flightInfo)
    {
        long affectedRows = 0;
        var doGetResult = _client.DoGetAsync(options, flightInfo.Endpoints.First().Ticket);
        await foreach (var recordBatch in doGetResult.ConfigureAwait(false))
        {
            affectedRows += recordBatch.Length;
        }

        return affectedRows;
    }
}

using System;
using System.Buffers;
using System.Threading;
using Grpc.Core;

namespace Apache.Arrow.Flight.Sql;

public class FlightCallOptions
{
    public FlightCallOptions()
    {
        Timeout = TimeSpan.FromSeconds(-1);
    }
    // Implement any necessary options for RPC calls
    public Metadata Headers { get; set; } = new();

    /// <summary>
    /// Gets or sets a token to enable interactive user cancellation of long-running requests.
    /// </summary>
    public CancellationToken StopToken { get; set; }

    /// <summary>
    /// Gets or sets the optional timeout for this call.
    /// Negative durations mean an implementation-defined default behavior will be used instead.
    /// </summary>
    public TimeSpan Timeout { get; set; }

    /// <summary>
    /// Gets or sets an optional memory manager to control where to allocate incoming data.
    /// </summary>
    public MemoryManager<ArrowBuffer>? MemoryManager { get; set; }
}

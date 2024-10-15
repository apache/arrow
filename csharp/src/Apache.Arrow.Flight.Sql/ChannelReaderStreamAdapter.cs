using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Grpc.Core;

namespace Apache.Arrow.Flight.Sql;

internal class ChannelReaderStreamAdapter<T> : IAsyncStreamReader<T>
{
    private readonly ChannelReader<T> _channelReader;

    public ChannelReaderStreamAdapter(ChannelReader<T> channelReader)
    {
        _channelReader = channelReader ?? throw new ArgumentNullException(nameof(channelReader));
        Current = default!;
    }

    public T Current { get; private set; }

    public async Task<bool> MoveNext(CancellationToken cancellationToken)
    {
        if (await _channelReader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (_channelReader.TryRead(out var item))
            {
                Current = item;
                return true;
            }
        }

        return false;
    }

    public void Dispose()
    {
        // No additional cleanup is required here since we are using a channel
    }
}
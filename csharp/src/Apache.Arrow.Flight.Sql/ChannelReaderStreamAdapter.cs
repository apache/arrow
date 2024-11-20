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
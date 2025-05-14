// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Middleware.Interfaces;
using Grpc.Core;

namespace Apache.Arrow.Flight.Middleware.Interceptors;

public class MiddlewareResponseStream<T> : IAsyncStreamReader<T> where T : class
{
    private readonly IAsyncStreamReader<T> _inner;
    private readonly AsyncServerStreamingCall<T> _call;
    private readonly List<IFlightClientMiddleware> _middlewareList;

    public MiddlewareResponseStream(
        IAsyncStreamReader<T> inner,
        AsyncServerStreamingCall<T> call,
        List<IFlightClientMiddleware> middlewareList)
    {
        _inner = inner;
        _call = call;
        _middlewareList = middlewareList;
    }

    public T Current => _inner.Current;

    public async Task<bool> MoveNext(CancellationToken cancellationToken)
    {
        try
        {
            bool hasNext = await _inner.MoveNext(cancellationToken).ConfigureAwait(false);
            if (!hasNext)
            {
                TriggerOnCallCompleted();
            }

            return hasNext;
        }
        catch
        {
            TriggerOnCallCompleted();
            throw;
        }
    }

    private void TriggerOnCallCompleted()
    {
        var status = _call.GetStatus();
        var trailers = _call.GetTrailers();

        foreach (var m in _middlewareList)
            m?.OnCallCompleted(status, trailers);
    }
}
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
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Apache.Arrow.Flight.Internal
{

    /// <summary>
    /// This is a helper class that allows conversions from gRPC types to the Arrow types.
    /// It maintains the stream so data can be read as soon as possible.
    /// </summary>
    /// <typeparam name="TIn">In paramter from gRPC</typeparam>
    /// <typeparam name="TOut">The arrow type returned</typeparam>
    internal class StreamReader<TIn, TOut> : IAsyncStreamReader<TOut>
    {
        private readonly IAsyncStreamReader<TIn> _inputStream;
        private readonly Func<TIn, TOut> _convertFunction;
        internal StreamReader(IAsyncStreamReader<TIn> inputStream, Func<TIn, TOut> convertFunction)
        {
            _inputStream = inputStream;
            _convertFunction = convertFunction;
        }

        public TOut Current { get; private set; }

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            var moveNextResult = await _inputStream.MoveNext(cancellationToken).ConfigureAwait(false);
            if (moveNextResult)
            {
                Current = _convertFunction(_inputStream.Current);
            }
            return moveNextResult;
        }
    }
}

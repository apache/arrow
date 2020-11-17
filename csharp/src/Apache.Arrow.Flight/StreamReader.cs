using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Apache.Arrow.Flight.Client
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
            var moveNextResult = await _inputStream.MoveNext(cancellationToken);
            if (moveNextResult)
            {
                Current = _convertFunction(_inputStream.Current);
            }
            return moveNextResult;
        }
    }
}

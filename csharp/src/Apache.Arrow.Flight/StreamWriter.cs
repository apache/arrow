using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;

namespace Apache.Arrow.Flight.Client
{
    public class StreamWriter<TIn, TOut> : IAsyncStreamWriter<TIn>
    {
        private readonly IAsyncStreamWriter<TOut> _inputStream;
        private readonly Func<TIn, TOut> _convertFunction;
        internal StreamWriter(IAsyncStreamWriter<TOut> inputStream, Func<TIn, TOut> convertFunction)
        {
            _inputStream = inputStream;
            _convertFunction = convertFunction;
        }

        public WriteOptions WriteOptions
        {
            get
            {
                return _inputStream.WriteOptions;
            }
            set
            {
                _inputStream.WriteOptions = value;
            }
        }

        public Task WriteAsync(TIn message)
        {
            return _inputStream.WriteAsync(_convertFunction(message));
        }
    }
}

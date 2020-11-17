using System;
using System.Collections.Generic;
using System.Text;
using Google.Protobuf;

namespace Apache.Arrow.Flight
{
    public class Result
    {
        private readonly Protocol.Result _result;
        public Result(Protocol.Result result)
        {
            _result = result;
        }

        public Result(ByteString ticket)
        {
            _result = new Protocol.Result()
            {
                Body = ticket
            };
        }

        public Result(string ticket)
            : this(ByteString.CopyFromUtf8(ticket))
        {
        }

        public Result(byte[] bytes)
            : this(ByteString.CopyFrom(bytes))
        {
        }

        public string ResultString => _result.Body.ToStringUtf8();

        public ByteString ResultByteString => _result.Body;

        public byte[] ResultBytes => _result.Body.ToByteArray();

        public Protocol.Result ToProtocol()
        {
            return _result;
        }
    }
}

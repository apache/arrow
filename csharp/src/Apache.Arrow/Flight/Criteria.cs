using System;
using System.Collections.Generic;
using System.Text;
using Google.Protobuf;

namespace Apache.Arrow.Flight
{
    public class Criteria
    {
        private readonly Protocol.Criteria _criteria;
        public Criteria(Protocol.Criteria criteria)
        {
            _criteria = criteria;
        }

        public Criteria()
        {
            _criteria = new Protocol.Criteria();
        }

        public Criteria(string expression)
        {
            _criteria = new Protocol.Criteria()
            {
                Expression = ByteString.CopyFromUtf8(expression)
            };
        }

        public Criteria(byte[] bytes)
        {
            _criteria = new Protocol.Criteria()
            {
                Expression = ByteString.CopyFrom(bytes)
            };
        }

        public Criteria(ByteString byteString)
        {
            _criteria = new Protocol.Criteria()
            {
                Expression = byteString
            };
        }

        public Protocol.Criteria ToProtocol()
        {
            return _criteria;
        }
    }
}

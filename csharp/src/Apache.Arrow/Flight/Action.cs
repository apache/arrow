using System;
using System.Collections.Generic;
using System.Text;
using Google.Protobuf;

namespace Apache.Arrow.Flight
{
    public class Action
    {
        private readonly Protocol.Action _action;
        public Action(Protocol.Action action)
        {
            _action = action;
        }

        public Action(string type, ByteString body)
        {
            _action = new Protocol.Action()
            {
                Body = body,
                Type = type
            };
        }

        public Action(string type, string body)
        {
            _action = new Protocol.Action()
            {
                Body = ByteString.CopyFromUtf8(body),
                Type = type
            };
        }

        public Action(string type, byte[] body)
        {
            _action = new Protocol.Action()
            {
                Body = ByteString.CopyFrom(body),
                Type = type
            };
        }

        public string Type => _action.Type;

        public string BodyString => _action.Body.ToStringUtf8();

        public byte[] BodyBytes => _action.Body.ToByteArray();

        public ByteString Body => _action.Body;

        public Protocol.Action ToProtocol()
        {
            return _action;
        }
    }
}

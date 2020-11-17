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

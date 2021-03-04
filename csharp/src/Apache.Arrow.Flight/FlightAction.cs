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
    public class FlightAction
    {
        private readonly Protocol.Action _action;
        internal FlightAction(Protocol.Action action)
        {
            _action = action;
        }

        public FlightAction(string type, ByteString body)
        {
            _action = new Protocol.Action()
            {
                Body = body,
                Type = type
            };
        }

        public FlightAction(string type, string body)
        {
            _action = new Protocol.Action()
            {
                Body = ByteString.CopyFromUtf8(body),
                Type = type
            };
        }

        public FlightAction(string type, byte[] body)
        {
            _action = new Protocol.Action()
            {
                Body = ByteString.CopyFrom(body),
                Type = type
            };
        }

        public FlightAction(string type)
        {
            _action = new Protocol.Action()
            {
                Type = type
            };
        }

        public string Type => _action.Type;

        public ByteString Body => _action.Body;

        internal Protocol.Action ToProtocol()
        {
            return _action;
        }
    }
}

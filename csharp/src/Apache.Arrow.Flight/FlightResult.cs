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
    public class FlightResult
    {
        private readonly Protocol.Result _result;

        internal FlightResult(Protocol.Result result)
        {
            _result = result;
        }

        public FlightResult(ByteString body)
        {
            _result = new Protocol.Result()
            {
                Body = body
            };
        }

        public FlightResult(string body)
            : this(ByteString.CopyFromUtf8(body))
        {
        }

        public FlightResult(byte[] body)
            : this(ByteString.CopyFrom(body))
        {
        }

        public ByteString Body => _result.Body;

        internal Protocol.Result ToProtocol()
        {
            return _result;
        }

        public override bool Equals(object obj)
        {
            if(obj is FlightResult other)
            {
                return Equals(_result, other._result);
            }
            return false;
        }

        public override int GetHashCode()
        {
            return _result.GetHashCode();
        }
    }
}

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

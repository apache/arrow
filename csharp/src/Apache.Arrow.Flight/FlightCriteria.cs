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
    public class FlightCriteria
    {
        internal static readonly FlightCriteria Empty = new FlightCriteria();

        private readonly Protocol.Criteria _criteria;

        internal FlightCriteria(Protocol.Criteria criteria)
        {
            _criteria = criteria;
        }

        public FlightCriteria()
        {
            _criteria = new Protocol.Criteria();
        }

        public FlightCriteria(string expression)
        {
            _criteria = new Protocol.Criteria()
            {
                Expression = ByteString.CopyFromUtf8(expression)
            };
        }

        public FlightCriteria(byte[] bytes)
        {
            _criteria = new Protocol.Criteria()
            {
                Expression = ByteString.CopyFrom(bytes)
            };
        }

        public FlightCriteria(ByteString byteString)
        {
            _criteria = new Protocol.Criteria()
            {
                Expression = byteString
            };
        }

        public ByteString Expression => _criteria.Expression;

        internal Protocol.Criteria ToProtocol()
        {
            return _criteria;
        }
    }
}

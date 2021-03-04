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
    public class FlightPutResult
    {
        public static readonly FlightPutResult Empty = new FlightPutResult();

        private readonly Protocol.PutResult _putResult;

        public FlightPutResult()
        {
            _putResult = new Protocol.PutResult();
        }

        public FlightPutResult(ByteString applicationMetadata)
        {
            _putResult = new Protocol.PutResult()
            {
                AppMetadata = applicationMetadata
            };
        }

        public FlightPutResult(byte[] applicationMetadata)
            : this(ByteString.CopyFrom(applicationMetadata))
        {
        }

        public FlightPutResult(string applicationMetadata)
            : this(ByteString.CopyFromUtf8(applicationMetadata))
        {
        }

        internal FlightPutResult(Protocol.PutResult putResult)
        {
            _putResult = putResult;
        }

        public ByteString ApplicationMetadata => _putResult.AppMetadata;

        internal Protocol.PutResult ToProtocol()
        {
            return _putResult;
        }
    }
}

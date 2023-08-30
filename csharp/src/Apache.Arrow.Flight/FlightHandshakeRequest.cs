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

using Google.Protobuf;

namespace Apache.Arrow.Flight;

public class FlightHandshakeRequest
{
    private readonly Protocol.HandshakeRequest _result;
    public ByteString Payload => _result.Payload;
    public ulong ProtocolVersion => _result.ProtocolVersion;

    internal FlightHandshakeRequest(Protocol.HandshakeRequest result)
    {
        _result = result;
    }

    public FlightHandshakeRequest(ByteString payload, ulong protocolVersion = 1)
    {
        _result = new Protocol.HandshakeRequest
        {
            Payload = payload,
            ProtocolVersion = protocolVersion
        };
    }

    internal Protocol.HandshakeRequest ToProtocol()
    {
        return _result;
    }

    public override bool Equals(object obj)
    {
        if(obj is FlightHandshakeRequest other)
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

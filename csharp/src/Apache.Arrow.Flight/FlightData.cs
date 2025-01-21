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

public class FlightData
{
    public FlightDescriptor Descriptor { get; }
    public ByteString AppMetadata { get; }
    public ByteString DataBody { get; }
    public ByteString DataHeader { get; }

    public FlightData(FlightDescriptor descriptor, ByteString dataBody = null, ByteString dataHeader = null, ByteString appMetadata = null)
    {
        Descriptor = descriptor;
        DataBody = dataBody;
        DataHeader = dataHeader;
        AppMetadata = appMetadata;
    }

    internal FlightData(Protocol.FlightData protocolFlightData)
    {
        Descriptor = protocolFlightData.FlightDescriptor == null ? null : new FlightDescriptor(protocolFlightData.FlightDescriptor);
        DataBody = protocolFlightData.DataBody;
        DataHeader = protocolFlightData.DataHeader;
        AppMetadata = protocolFlightData.AppMetadata;
    }

    internal Protocol.FlightData ToProtocol()
    {
        return new Protocol.FlightData
        {
            FlightDescriptor = Descriptor?.ToProtocol(),
            AppMetadata = AppMetadata,
            DataBody = DataBody,
            DataHeader = DataHeader
        };
    }
}

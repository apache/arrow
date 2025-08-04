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
using Apache.Arrow.Flight.Protocol;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Apache.Arrow.Flight;

public class FlightInfoCancelRequest : IMessage
{
    private readonly CancelFlightInfoRequest _cancelFlightInfoRequest;
    public FlightInfo FlightInfo { get; private set; }

    public FlightInfoCancelRequest(FlightInfo flightInfo)
    {
        FlightInfo = flightInfo ?? throw new ArgumentNullException(nameof(flightInfo));
        _cancelFlightInfoRequest = new CancelFlightInfoRequest();
    }

    public FlightInfoCancelRequest()
    {
        _cancelFlightInfoRequest = new CancelFlightInfoRequest();
    }

    public void MergeFrom(CodedInputStream input)
    {
        _cancelFlightInfoRequest.MergeFrom(input);
    }

    public void WriteTo(CodedOutputStream output)
    {
        _cancelFlightInfoRequest.WriteTo(output);
    }

    public int CalculateSize() => _cancelFlightInfoRequest.CalculateSize();

    public MessageDescriptor Descriptor =>
        DescriptorReflection.Descriptor.MessageTypes[0];
}

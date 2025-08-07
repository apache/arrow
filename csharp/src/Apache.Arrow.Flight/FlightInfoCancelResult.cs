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

using Apache.Arrow.Flight.Protocol;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Apache.Arrow.Flight;

public class FlightInfoCancelResult : IMessage
{
    private readonly CancelFlightInfoResult _flightInfoCancelResult;

    public FlightInfoCancelResult()
    {
        _flightInfoCancelResult = new CancelFlightInfoResult();
        Descriptor = DescriptorReflection.Descriptor.MessageTypes[0];
    }

    public void MergeFrom(CodedInputStream input) => _flightInfoCancelResult.MergeFrom(input);

    public void WriteTo(CodedOutputStream output) => _flightInfoCancelResult.WriteTo(output);

    public int CalculateSize()
    {
        return _flightInfoCancelResult.CalculateSize();
    }

    public MessageDescriptor Descriptor { get; }

    public int GetCancelStatus()
    {
        return (int)_flightInfoCancelResult.Status;
    }

    public void SetStatus(int status)
    {
        _flightInfoCancelResult.Status = (CancelStatus)status;
    }
}

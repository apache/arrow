﻿// Licensed to the Apache Software Foundation (ASF) under one or more
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
using System.Linq;
using System.Text;
using Google.Protobuf;

namespace Apache.Arrow.Flight
{
    public class FlightDescriptor
    {
        private readonly Protocol.FlightDescriptor _flightDescriptor;

        private FlightDescriptor(ByteString command)
        {
            _flightDescriptor = new Protocol.FlightDescriptor()
            {
                Cmd = command,
                Type = Protocol.FlightDescriptor.Types.DescriptorType.Cmd
            };
        }

        private FlightDescriptor(params string[] paths)
        {
            _flightDescriptor = new Protocol.FlightDescriptor()
            {
                Type = Protocol.FlightDescriptor.Types.DescriptorType.Path
            };

            foreach(var path in paths)
            {
                _flightDescriptor.Path.Add(path);
            }
        }


        public static FlightDescriptor Command(byte[] command)
        {
            return new FlightDescriptor(ByteString.CopyFrom(command));
        }

        public static FlightDescriptor Command(string command)
        {
            return new FlightDescriptor(ByteString.CopyFromUtf8(command));
        }

        public static FlightDescriptor Path(params string[] paths)
        {
            return new FlightDescriptor(paths);
        }

        public static FlightDescriptor FromProtocol(Protocol.FlightDescriptor flightDescriptor)
        {
            return new FlightDescriptor(flightDescriptor);
        }


        internal FlightDescriptor(Protocol.FlightDescriptor flightDescriptor)
        {
            if(flightDescriptor.Type != Protocol.FlightDescriptor.Types.DescriptorType.Cmd && flightDescriptor.Type != Protocol.FlightDescriptor.Types.DescriptorType.Path)
            {
                throw new NotSupportedException();
            }
            _flightDescriptor = flightDescriptor;
        }

        public Protocol.FlightDescriptor ToProtocol()
        {
            return _flightDescriptor;
        }

        public bool IsCommand => _flightDescriptor.Type == Protocol.FlightDescriptor.Types.DescriptorType.Cmd;

        public IEnumerable<string> Paths => _flightDescriptor.Path;

        public ByteString CommandByteString => _flightDescriptor.Cmd;

        public byte[] CommandBytes => _flightDescriptor.Cmd.ToByteArray();

        public string CommandString => _flightDescriptor.Cmd.ToStringUtf8();

        public override int GetHashCode()
        {
            return _flightDescriptor.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if(obj is FlightDescriptor other)
            {
                return Equals(_flightDescriptor, other._flightDescriptor);
            }
            return false;
        }
    }
}

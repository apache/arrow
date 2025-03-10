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
using System.Buffers;
using System.Threading;
using Grpc.Core;

namespace Apache.Arrow.Flight.Sql;

public class FlightCallOptions
{
    public FlightCallOptions()
    {
        Timeout = TimeSpan.FromSeconds(-1);
    }
    
    // Implement any necessary options for RPC calls
    public Metadata Headers { get; set; } = new();

    /// <summary>
    /// Gets or sets the optional timeout for this call.
    /// Negative durations mean an implementation-defined default behavior will be used instead.
    /// </summary>
    public TimeSpan Timeout { get; set; }
}

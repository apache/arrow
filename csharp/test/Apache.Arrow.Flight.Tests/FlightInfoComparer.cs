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

using Apache.Arrow.Tests;
using Xunit;

namespace Apache.Arrow.Flight.Tests
{
    public static class FlightInfoComparer
    {
        public static void Compare(FlightInfo expected, FlightInfo actual)
        {
            //Check endpoints
            Assert.Equal(expected.Endpoints, actual.Endpoints);

            //Check flight descriptor
            Assert.Equal(expected.Descriptor, actual.Descriptor);

            //Check schema
            SchemaComparer.Compare(expected.Schema, actual.Schema);

            Assert.Equal(expected.TotalBytes, actual.TotalBytes);

            Assert.Equal(expected.TotalRecords, actual.TotalRecords);
        }
    }
}

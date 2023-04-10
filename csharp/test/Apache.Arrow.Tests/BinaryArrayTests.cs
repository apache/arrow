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
using Xunit;

namespace Apache.Arrow.Tests
{
    public class BinaryArrayTests
    {
        public class IEnumerableArray
        {
            [Fact]
            public void BinaryArray_ShouldBe_IEnumerable()
            {
                byte[] bvalue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 };

                // Build test array
                BinaryArray array = new BinaryArray.Builder()
                    .Append(bvalue.AsSpan<byte>()).AppendNull()
                    .Build();

                byte[][] expected = new byte[][] { bvalue, null };

                int i = 0;
                foreach (byte[] value in array)
                {
                    Assert.Equal(expected[i], value);
                    i++;
                }
            }
        }
    }
}

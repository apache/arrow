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

using System.Linq;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class PrimitiveArrayTests
    {
        public class IEnumerableArray
        {
            [Fact]
            public void PrimitiveArray_ShouldBe_IEnumerable()
            {
                // Build test array
                Int32Array array = new Int32Array.Builder()
                    .Append(1).AppendNull().Append(-12)
                    .Build();

                int?[] expected = new int?[] { 1, null, -12 };

                int i = 0;
                foreach (int? value in array)
                {
                    Assert.Equal(expected[i], value);
                    i++;
                }
            }
        }

        public class IEnumeratorArray
        {
            [Fact]
            public void PrimitiveArray_Should_Use_System_Linq()
            {
                // Build test array
                Int32Array array = new Int32Array.Builder()
                    .Append(1).AppendNull().Append(-12)
                    .Build();

                Assert.Equal(new int?[] { 2 }, array.Select(i => i * 2).Where(i => i == 2).ToArray());
                Assert.Equal(array.Sum(), -11);
            }
        }
    }
}

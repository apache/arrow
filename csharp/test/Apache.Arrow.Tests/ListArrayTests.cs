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

using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ListArrayTests
    {
        [Fact]
        public void ListArray_Should_Build()
        {
            string[][] expected = new string[][]
            {
                    new string[] { "john", null, "doe" },
                    new string[] { null },
                    new string[] { "elon", "musk" },
                    null,
                    System.Array.Empty<string>(),
            };

            ListArray array = new ListArray.Builder(new StringType()).AppendRange(expected).Build();

            // Assert
            Assert.IsType<ListArray>(array);
            Assert.Equal(expected.Length, array.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                if (expected[i] == null)
                {
                    Assert.True(array.IsNull(i));
                }
                else
                {
                    Assert.Equal(expected[i].Length, array.GetArray<StringArray>(i).Length);
                    StringArray values = array.GetArray<StringArray>(i);
                    Assert.Equal(expected[i], values.ToArray());
                }
            }
        }
    }
}

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
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ListArrayTests
    {
        public class IEnumerableArray
        {
            [Fact]
            public void ListArray_ShouldBe_IEnumerable()
            {
                // Build test array
                var builder = new ListArray.Builder(new StringType());
                var valueBuilder = (StringArray.Builder)builder.ValueBuilder;

                // Populate
                builder.Append();
                valueBuilder.Append("john").AppendNull().Append("doe");
                builder.Append();
                valueBuilder.AppendNull().AppendNull().AppendNull();
                builder.Append();
                valueBuilder.AppendNull().Append("elon").Append("musk");

                ListArray array = builder.Build();

                string[][] expected = new string[][]
                {
                    new string[] { "john", null, "doe" },
                    new string[] { null, null, null },
                    new string[] { null, "elon", "musk" }
                };

                // Assert
                Assert.Equal(expected.Length, array.Length);

                int i = 0;
                foreach (IArrowArray value in array)
                {
                    var values = (StringArray)value;
                    Assert.Equal(expected[i], values.ToArray());
                    i++;
                }
            }
        }
    }
}

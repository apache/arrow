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

using Xunit;

namespace Apache.Arrow.Tests;

public class StringArrayBuilderTests
{
    public class Set
    {
        [Theory]
        [InlineData(1, "Test5")]
        [InlineData(0, "T5")]
        [InlineData(1, "T5")]
        [InlineData(2, "T5")]
        [InlineData(2, "Test3")]
        [InlineData(1, "NewTest")]
        [InlineData(2, "NewTest")]
        [InlineData(0, "NewTest")]
        public void EnsureSetsUpdatesTheCorrectStrings(int offset, string newValue)
        {
            // Arrange
            var builder = new StringArray.Builder();
            var defaultValues = new[] { "Test",  "Test1", "Test2"};
            var expectedValues = (string[])defaultValues.Clone();
            expectedValues[offset] = newValue;
            foreach (string defaultValue in defaultValues)
            {
                builder.Append(defaultValue);
            }

            // Act
            builder.Set(offset, newValue);
            var array = builder.Build();

            // Assert
            for (int i = 0; i <expectedValues.Length; i++)
            {
                string actual = array.GetString(i);
                Assert.Equal(expectedValues[i], actual);
            }
        }
    }
}

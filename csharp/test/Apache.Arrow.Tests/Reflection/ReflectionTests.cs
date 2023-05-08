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

using System.Collections.Generic;
using Apache.Arrow.Reflection;
using Xunit;

namespace Apache.Arrow.Tests.Reflection
{
    public class ReflectionTests
    {
        [Theory]
        [InlineData("abc", -1, 1.23)]
        [InlineData(null, null, 0)]
        [InlineData("abc", null, -2.36)]
        [InlineData(null, 42, 42)]
        public void TypeReflection_Should_GetStructValues(string str, int? i, double dbl)
        {
            var expected = new RecordStruct { String = str, NullableInt = i, Double = dbl };
            var result = TypeReflection<RecordStruct>.PropertyValues(expected);

            Assert.Equal(result, new object[] { expected.String, expected.NullableInt, expected.Double });
        }

        [Theory]
        [InlineData("abc", -1)]
        [InlineData(null, null)]
        [InlineData("abc", null)]
        [InlineData(null, 42)]
        public void TypeReflection_Should_KeyValuePair(string str, int? i)
        {
            var expected = new KeyValuePair<string, int?>(str, i);
            var result = TypeReflection<KeyValuePair<string, int?>>.PropertyValues(expected);

            Assert.Equal(result, new object[] { expected.Key, expected.Value });
        }
    }

    public class DotNetScalarTests
    {
        [Theory]
        [InlineData("abc", "def")]
        public void DotNetScalar_Should_Make(string p0, string p1)
        {
            var expected = new DynamicStruct { p0 = p0, p1 = p1 };
            var result = DotNetScalar.Make(expected);

            Assert.Equal(result.ArrowType, TypeReflection<DynamicStruct>.ArrowType);
            Assert.True(result.IsValid);
            Assert.Equal(result.Value, expected);
            Assert.Equal(result.Values, new object[] { expected.p0, expected.p1 });
        }
    }


    public struct DynamicStruct
    {
        public string p0 { get; set; }
        public string p1 { get; set; }
    }

    public struct RecordStruct
    {
        public string String { get; set; }
        public int? NullableInt { get; set; }
        public double Double { get; set; }
    }
}

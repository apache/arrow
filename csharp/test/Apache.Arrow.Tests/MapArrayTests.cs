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
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class MapArrayTests
    {
        [Fact]
        public void MapArray_Should_GetTuple()
        {
            MapType type = new MapType(StringType.Default, Int64Type.Default);
            MapArray.Builder builder = new MapArray.Builder(type);
            var keyBuilder = builder.KeyBuilder as StringArray.Builder;
            var valueBuilder = builder.ValueBuilder as Int64Array.Builder;

            Tuple<string, long?> kv0 = Tuple.Create("test", (long?)1);
            Tuple<string, long?> kv1 = Tuple.Create("other", (long?)123);
            Tuple<string, long?> kv2 = Tuple.Create("kv", (long?)null);

            builder.Append();
            keyBuilder.Append("test");
            valueBuilder.Append(1);

            builder.AppendNull();

            builder.Append();
            keyBuilder.Append("other");
            valueBuilder.Append(123);
            keyBuilder.Append("kv");
            valueBuilder.AppendNull();

            MapArray array = builder.Build();

            Assert.Equal(new Tuple<string, long?>[] { kv0 }, array.GetTuples<StringArray, string, Int64Array, long?>(0, GetKey, GetValue).ToArray());
            Assert.True(array.IsNull(1));
            Assert.Equal(new Tuple<string, long?>[] { kv1, kv2 }, array.GetTuples<StringArray, string, Int64Array, long?>(2, GetKey, GetValue).ToArray());
        }

        [Fact]
        public void MapArray_Should_GetKeyValuePairs()
        {
            MapType type = new MapType(StringType.Default, Int32Type.Default);
            MapArray.Builder builder = new MapArray.Builder(type);
            var keyBuilder = builder.KeyBuilder as StringArray.Builder;
            var valueBuilder = builder.ValueBuilder as Int32Array.Builder;

            KeyValuePair<string, int?> kv0 = new KeyValuePair<string, int?>("test", (int?)1);
            KeyValuePair<string, int?> kv1 = new KeyValuePair<string, int?>("other", (int?)123);
            KeyValuePair<string, int?> kv2 = new KeyValuePair<string, int?>("kv", (int?)null);

            builder.Append();
            keyBuilder.Append("test");
            valueBuilder.Append(1);

            builder.AppendNull();

            builder.Append();
            keyBuilder.Append("other");
            valueBuilder.Append(123);
            keyBuilder.Append("kv");
            valueBuilder.AppendNull();

            MapArray array = builder.Build();

            Assert.Equal(new KeyValuePair<string, int?>[] { kv0 }, array.GetKeyValuePairs<StringArray, string, Int32Array, int?>(0, GetKey, GetValue).ToArray());
            Assert.True(array.IsNull(1));
            Assert.Equal(new KeyValuePair<string, int?>[] { kv1, kv2 }, array.GetKeyValuePairs<StringArray, string, Int32Array, int?>(2, GetKey, GetValue).ToArray());
        }

        private static string GetKey(StringArray array, int index) => array.GetString(index);
        private static int? GetValue(Int32Array array, int index) => array.GetValue(index);
        private static long? GetValue(Int64Array array, int index) => array.GetValue(index);
    }
}

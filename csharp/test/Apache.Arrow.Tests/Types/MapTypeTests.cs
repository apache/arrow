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

namespace Apache.Arrow.Tests.Types
{
    public class MapTypeTests
    {
        [Fact]
        public void MapType_Should_HaveCorrectTypeId()
        {
            var type = new MapType(StringType.Default, Int32Type.Default);
            Assert.Equal(ArrowTypeId.Map, type.TypeId);
        }

        [Fact]
        public void MapType_Should_HaveCorrectStructType()
        {
            var type = new MapType(BooleanType.Default, Int32Type.Default, true);
            Assert.IsType<StructType>(type.Fields[0].DataType);
            Assert.Equal(2, type.KeyValueType.Fields.Count);

            Assert.Equal("entries", type.Fields[0].Name);
            Assert.Equal("key", type.KeyField.Name);
            Assert.Equal("value", type.ValueField.Name);

            Assert.False(type.Fields[0].IsNullable);
            Assert.False(type.KeyField.IsNullable);
            Assert.True(type.ValueField.IsNullable);
            Assert.False(new MapType(BooleanType.Default, Int32Type.Default, false).ValueField.IsNullable);

            Assert.IsType<BooleanType>(type.KeyField.DataType);
            Assert.IsType<Int32Type>(type.ValueField.DataType);
        }

        [Fact]
        public void MapType_Should_SetKeySorted()
        {
            Assert.False(new MapType(BooleanType.Default, Int32Type.Default).KeySorted);
            Assert.True(new MapType(StringType.Default, Int32Type.Default, true, true).KeySorted);
        }
    }
}

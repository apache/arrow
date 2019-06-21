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
    public class ColumnTests
    {
        public static Array MakeIntArray(int length)
        {
            // The following should be improved once the ArrayBuilder PR goes in
            var intBuilder = new ArrowBuffer.Builder<int>();
            intBuilder.AppendRange(Enumerable.Range(0, length).Select(x => x));
            ArrowBuffer buffer = intBuilder.Build();
            ArrayData intData = new ArrayData(Int32Type.Default, length, 0, 0, new[] { ArrowBuffer.Empty, buffer });
            Array intArray = ArrowArrayFactory.BuildArray(intData) as Array;
            return intArray;
        }

        [Fact]
        public void TestColumn()
        {
            Array intArray = MakeIntArray(10);
            Array intArrayCopy = MakeIntArray(10);

            Field field = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
            Column column = new Column(field, new[] { intArray, intArrayCopy });

            Assert.True(column.Name == field.Name);
            Assert.True(column.Field == field);
            Assert.Equal(20, column.Length);
            Assert.Equal(0, column.NullCount);
            Assert.Equal(field.DataType, column.Type);
            
            Column slice5 = column.Slice(0, 5);
            Assert.Equal(5, slice5.Length);
            Column sliceFull = column.Slice(2);
            Assert.Equal(18, sliceFull.Length);
            Column sliceMore = column.Slice(0, 25);
            Assert.Equal(20, sliceMore.Length);
        }
    }
}

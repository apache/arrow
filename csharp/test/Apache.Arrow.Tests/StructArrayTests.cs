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
using System.Collections.Generic;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class StructArrayTests
    {
        [Fact]
        public void TestStructArray()
        {
            // The following can be improved with a Builder class for StructArray.
            List<Field> fields = new List<Field>();
            Field.Builder fieldBuilder = new Field.Builder();
            fields.Add(fieldBuilder.Name("Strings").DataType(StringType.Default).Nullable(true).Build());
            fieldBuilder = new Field.Builder();
            fields.Add(fieldBuilder.Name("Ints").DataType(Int32Type.Default).Nullable(true).Build());
            StructType structType = new StructType(fields);

            StringArray.Builder stringBuilder = new StringArray.Builder();
            StringArray stringArray = stringBuilder.Append("joe").AppendNull().AppendNull().Append("mark").Build();
            Int32Array.Builder intBuilder = new Int32Array.Builder();
            Int32Array intArray = intBuilder.Append(1).Append(2).AppendNull().Append(4).Build();
            List<Array> arrays = new List<Array>();
            arrays.Add(stringArray);
            arrays.Add(intArray);

            ArrowBuffer.BitmapBuilder nullBitmap = new ArrowBuffer.BitmapBuilder();
            var nullBitmapBuffer = nullBitmap.Append(true).Append(true).Append(false).Append(true).Build();
            StructArray structs = new StructArray(structType, 4, arrays, nullBitmapBuffer, 1);

            Assert.Equal(4, structs.Length);
            Assert.Equal(1, structs.NullCount);
            ArrayData[] childArrays = structs.Data.Children; // Data for StringArray and Int32Array
            Assert.Equal(2, childArrays.Length);
            for (int i = 0; i < childArrays.Length; i++)
            {
                ArrayData arrayData = childArrays[i];
                Assert.Null(arrayData.Children);
                if (i == 0)
                {
                    Assert.Equal(ArrowTypeId.String, arrayData.DataType.TypeId);
                    Array array = new StringArray(arrayData);
                    StringArray structStringArray = array as StringArray;
                    Assert.NotNull(structStringArray);
                    Assert.Equal(structs.Length, structStringArray.Length);
                    Assert.Equal(stringArray.Length, structStringArray.Length);
                    Assert.Equal(stringArray.NullCount, structStringArray.NullCount);
                    for (int j = 0; j < stringArray.Length; j++)
                    {
                        Assert.Equal(stringArray.GetString(j), structStringArray.GetString(j));
                    }
                }
                if (i == 1)
                {
                    Assert.Equal(ArrowTypeId.Int32, arrayData.DataType.TypeId);
                    Array array = new Int32Array(arrayData);
                    Int32Array structIntArray = array as Int32Array;
                    Assert.NotNull(structIntArray);
                    Assert.Equal(structs.Length, structIntArray.Length);
                    Assert.Equal(intArray.Length, structIntArray.Length);
                    Assert.Equal(intArray.NullCount, structIntArray.NullCount);
                    for (int j = 0; j < intArray.Length; j++)
                    {
                        Assert.Equal(intArray.GetValue(j), structIntArray.GetValue(j));
                    }
                }
            }
        }
    }
}

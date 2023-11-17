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
    public class IStructTests
    {
        [Fact]
        public void StructsAndRecordBatchesAreSimilar()
        {
            Field stringField = new Field("column1", StringType.Default, true);
            StringArray.Builder stringBuilder = new StringArray.Builder();
            StringArray stringArray = stringBuilder.Append("joe").AppendNull().AppendNull().Append("mark").Build();

            Field intField = new Field("column2", Int32Type.Default, true);
            Int32Array.Builder intBuilder = new Int32Array.Builder();
            Int32Array intArray = intBuilder.Append(1).Append(2).AppendNull().Append(4).Build();

            Schema schema = new Schema(new[] { stringField, intField }, null);
            RecordBatch batch = new RecordBatch(schema, new IArrowArray[] { stringArray, intArray }, intArray.Length);
            IArrowStructArray structArray1 = batch;

            StructType structType = new StructType(new[] { stringField, intField });
            StructArray structArray = new StructArray(structType, intArray.Length, new IArrowArray[] { stringArray, intArray }, ArrowBuffer.Empty);
            IArrowStructArray structArray2 = structArray;

            FieldComparer.Compare(structArray1.Schema, structArray2.Schema);
            Assert.Equal(structArray1.Length, structArray2.Length);
            Assert.Equal(structArray1.ColumnCount, structArray2.ColumnCount);
            Assert.Equal(structArray1.NullCount, structArray2.NullCount);

            for (int i = 0; i < structArray1.ColumnCount; i++)
            {
                ArrowReaderVerifier.CompareArrays(structArray1.Column(i), structArray2.Column(i));
            }
        }
    }
}

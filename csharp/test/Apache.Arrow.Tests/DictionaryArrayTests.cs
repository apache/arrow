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
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class DictionaryArrayTests
    {
        [Fact]
        public void CreateTest()
        {
            (StringArray originalDictionary, Int32Array originalIndicesArray, DictionaryArray dictionaryArray) =
                CreateSimpleTestData();

            Assert.Equal(dictionaryArray.Dictionary, originalDictionary);
            Assert.Equal(dictionaryArray.Indices, originalIndicesArray);
        }

        [Fact]
        public void SliceTest()
        {
            (StringArray originalDictionary, Int32Array originalIndicesArray, DictionaryArray dictionaryArray) =
                CreateSimpleTestData();

            int batchLength = originalIndicesArray.Length;
            for (int offset = 0; offset < batchLength; offset++)
            {
                for (int length = 1; offset + length <= batchLength; length++)
                {
                    var sliced = dictionaryArray.Slice(offset, length) as DictionaryArray;
                    var actualSlicedDictionary = sliced.Dictionary as StringArray;
                    var actualSlicedIndicesArray = sliced.Indices as Int32Array;

                    var expectedSlicedIndicesArray = originalIndicesArray.Slice(offset, length) as Int32Array;

                    //Dictionary is not sliced.
                    Assert.Equal(originalDictionary.Data, actualSlicedDictionary.Data);
                    Assert.Equal(expectedSlicedIndicesArray.ToList(), actualSlicedIndicesArray.ToList());
                }
            }
        }

        private Tuple<StringArray, Int32Array, DictionaryArray> CreateSimpleTestData()
        {
            StringArray originalDictionary = new StringArray.Builder().AppendRange(new[] { "a", "b", "c" }).Build();
            Int32Array originalIndicesArray = new Int32Array.Builder().AppendRange(new[] { 0, 0, 1, 1, 2, 2 }).Build();
            var dictionaryArray = new DictionaryArray(new DictionaryType(Int32Type.Default, StringType.Default, false), originalIndicesArray, originalDictionary);

            return Tuple.Create(originalDictionary, originalIndicesArray, dictionaryArray);
        }
    }
}

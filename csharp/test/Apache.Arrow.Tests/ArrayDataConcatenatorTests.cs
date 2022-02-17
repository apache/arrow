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
using System.Reflection;
using Apache.Arrow.Memory;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrayDataConcatenatorTests
    {
        [Fact]
        public void TestNullOrEmpty()
        {
            Assert.Null(ArrayDataConcatenatorReflector.InvokeConcatenate(null));
            Assert.Null(ArrayDataConcatenatorReflector.InvokeConcatenate(new List<ArrayData>()));
        }

        [Fact]
        public void TestSingleElement()
        {
            Int32Array array = new Int32Array.Builder().Append(1).Append(2).Build();
            ArrayData actualArray = ArrayDataConcatenatorReflector.InvokeConcatenate(new[] { array.Data });
            ArrowReaderVerifier.CompareArrays(array, ArrowArrayFactory.BuildArray(actualArray));
        }

        private static class ArrayDataConcatenatorReflector
        {
            private static readonly MethodInfo s_concatenateInfo = typeof(ArrayData).Assembly.GetType("Apache.Arrow.ArrayDataConcatenator")
                .GetMethod("Concatenate", BindingFlags.Static | BindingFlags.NonPublic);

            internal static ArrayData InvokeConcatenate(IReadOnlyList<ArrayData> arrayDataList, MemoryAllocator allocator = default)
            {
                return s_concatenateInfo.Invoke(null, new object[] { arrayDataList, allocator }) as ArrayData;
            }
        }
    }
}

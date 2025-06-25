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
using System.Linq;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests;

public class LargeListArrayTests
{
    [Fact]
    public void GetSlicedValuesReturnsCorrectValues()
    {
        var values = new int?[][]
        {
            new int?[] {0, 1, 2},
            System.Array.Empty<int?>(),
            null,
            new int?[] {3, 4, null, 6},
        };

        var array = BuildArray(values);

        Assert.Equal(values.Length, array.Length);
        for (int i = 0; i < values.Length; ++i)
        {
            Assert.Equal(values[i] == null, array.IsNull(i));
            var arrayItem = (Int32Array) array.GetSlicedValues(i);
            if (values[i] == null)
            {
                Assert.Null(arrayItem);
            }
            else
            {
                Assert.Equal(values[i], arrayItem.ToArray());
            }
        }
    }

    [Fact]
    public void GetSlicedValuesChecksForOffsetOverflow()
    {
        var valuesArray = new Int32Array.Builder().Build();
        var offsetBuffer = new ArrowBuffer.Builder<long>();
        var validityBuffer = new ArrowBuffer.BitmapBuilder();

        offsetBuffer.Append(0);
        offsetBuffer.Append((long)int.MaxValue + 1);
        validityBuffer.Append(true);

        var array = new LargeListArray(
            new LargeListType(new Int32Type()), length: 1,
            offsetBuffer.Build(), valuesArray, validityBuffer.Build(),
            validityBuffer.UnsetBitCount);

        Assert.Throws<OverflowException>(() => array.GetSlicedValues(0));
    }

    private static LargeListArray BuildArray(int?[][] values)
    {
        var valuesBuilder = new Int32Array.Builder();
        var offsetBuffer = new ArrowBuffer.Builder<long>();
        var validityBuffer = new ArrowBuffer.BitmapBuilder();

        long offset = 0;
        offsetBuffer.Append(offset);
        foreach (var listValue in values)
        {
            if (listValue == null)
            {
                validityBuffer.Append(false);
                offsetBuffer.Append(offset);
            }
            else
            {
                foreach (var value in listValue)
                {
                    valuesBuilder.Append(value);
                }
                offset += listValue.Length;
                offsetBuffer.Append(offset);
                validityBuffer.Append(true);
            }
        }

        return new LargeListArray(
            new LargeListType(new Int32Type()), values.Length,
            offsetBuffer.Build(), valuesBuilder.Build(), validityBuffer.Build(),
            validityBuffer.UnsetBitCount);
    }
}

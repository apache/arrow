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
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests;

public class FixedSizeBinaryArrayTests
{
    [Fact]
    public void SliceFixedSizeBinaryArray()
    {
        const int byteWidth = 2;
        const int length = 5;
        const int nullCount = 1;

        var validityBuffer = new ArrowBuffer.BitmapBuilder()
            .AppendRange(true, 2)
            .Append(false)
            .AppendRange(true, 2)
            .Build();
        var dataBuffer = new ArrowBuffer.Builder<byte>()
            .AppendRange(Enumerable.Range(0, length * byteWidth).Select(i => (byte)i))
            .Build();
        var arrayData = new ArrayData(
            new FixedSizeBinaryType(byteWidth),
            length, nullCount, 0, new [] {validityBuffer, dataBuffer});
        var array = new FixedSizeBinaryArray(arrayData);

        var slice = (FixedSizeBinaryArray)array.Slice(1, 3);

        Assert.Equal(3, slice.Length);
        Assert.Equal(new byte[] {2, 3}, slice.GetBytes(0).ToArray());
        Assert.True(slice.GetBytes(1).IsEmpty);
        Assert.Equal(new byte[] {6, 7}, slice.GetBytes(2).ToArray());
    }
}

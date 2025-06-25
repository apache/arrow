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

using Xunit;

namespace Apache.Arrow.Tests;

public class BinaryViewArrayTests
{
    [Fact]
    public void SliceBinaryViewArray()
    {
        var array = new BinaryViewArray.Builder()
            .Append(new byte[] { 0, 1, 2 })
            .Append(new byte[] { 3, 4 })
            .AppendNull()
            .Append(new byte[] { 5, 6 })
            .Append(new byte[] { 7, 8 })
            .Build();

        var slice = (BinaryViewArray)array.Slice(1, 3);

        Assert.Equal(3, slice.Length);
        Assert.Equal(new byte[] {3, 4}, slice.GetBytes(0).ToArray());
        Assert.True(slice.GetBytes(1).IsEmpty);
        Assert.Equal(new byte[] {5, 6}, slice.GetBytes(2).ToArray());
    }
}

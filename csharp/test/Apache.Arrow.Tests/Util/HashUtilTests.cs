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
using Apache.Arrow.Util;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class HashUtilTests
    {
        [Fact]
        public void Hash32_ShouldNot_OverFlow()
        {
            Assert.InRange(HashUtil.Hash32Array(new int[] { 1 }), int.MinValue, int.MaxValue);
            Assert.InRange(HashUtil.Hash32(""), int.MinValue, int.MaxValue);
            Assert.InRange(HashUtil.Hash32("abc", 1), int.MinValue, int.MaxValue);
        }

        [Fact]
        public void Hash32_Should_HashObjects()
        {
            Assert.Equal(HashUtil.Hash32("abc", 1), HashUtil.Hash32("abc", 1));
            Assert.NotEqual(HashUtil.Hash32("abc", 1), HashUtil.Hash32(1, "abc"));
        }

        [Fact]
        public void Hash32_Should_HashObjectsOver8()
        {
            Assert.InRange(HashUtil.Hash32(Enumerable.Range(0, 20).ToArray()), int.MinValue, int.MaxValue);
        }
    }
}

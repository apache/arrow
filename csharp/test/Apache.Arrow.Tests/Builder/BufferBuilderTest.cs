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

using Apache.Arrow.Builder;
using Xunit;

namespace Apache.Arrow.Tests.Builder
{
    public class BufferBuilderTests
    {
        [Fact]
        public void BufferBuilder_Should_Build()
        {
            var builder = new BufferBuilder(8);

            builder.AppendByte(0x01);

            var built = builder.Build();

            Assert.Equal(0x01, built.Span[0]);
            Assert.Equal(0x00, built.Span[1]);
            Assert.Equal(64, built.Length);
        }
    }
}

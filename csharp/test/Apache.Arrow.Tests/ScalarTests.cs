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
    public class Decimal128ScalarTests
    {
        [Fact]
        public void Decimal128Scalar_Should_BuildWithCustomType()
        {
            decimal expected = 123.00001m;
            var value = new Decimal128Scalar(new Decimal128Type(15,5), expected);

            Assert.Equal(expected, value.Value);
            Assert.Equal(new byte[] {
                225, 174, 187, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            }, value.View().ToArray());
        }

        [Fact]
        public void Decimal128Scalar_Should_BuildFromDotNet()
        {
            decimal expected = 123.00001m;
            var value = new Decimal128Scalar(expected);

            Assert.Equal(Decimal128Type.Default, value.Type);
            Assert.Equal(expected, value.Value);
            Assert.Equal(new byte[] {
                16, 53, 95, 163, 28, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            }, value.View().ToArray());
        }

        [Fact]
        public void Decimal128Scalar_Should_BuildFromNegativeDotNet()
        {
            decimal expected = -987.00001m;
            var value = new Decimal128Scalar(expected);

            Assert.Equal(Decimal128Type.Default, value.Type);
            Assert.Equal(expected, value.Value);
            Assert.Equal(new byte[] {
                240, 10, 55, 50, 26, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255
            }, value.View().ToArray());
        }
    }

    public class Decimal256ScalarTests
    {
        [Fact]
        public void Decimal256Scalar_Should_BuildWithCustomType()
        {
            decimal expected = 123.000000000000000000000001m;
            var value = new Decimal256Scalar(new Decimal256Type(32, 27), expected);

            Assert.Equal(expected, value.Value);
            Assert.Equal(new byte[] {
                232, 3, 0, 120, 67, 157, 45, 201, 32, 55, 111, 141, 1, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            }, value.View().ToArray());
        }

        [Fact]
        public void Decimal256Scalar_Should_BuildFromDotNet()
        {
            decimal expected = 123.000000000000000000000001m;
            var value = new Decimal256Scalar(expected);
            var bytes = new byte[] {
                16, 39, 0, 176, 162, 36, 200, 219, 71, 39, 88, 134, 15,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            };

            Assert.Equal(expected, value.Value);
            Assert.Equal(Decimal256Type.SystemDefault, value.Type);
            Assert.Equal(bytes, value.View().ToArray());
        }

        [Fact]
        public void Decimal256Scalar_Should_BuildFromNegativeDotNet()
        {
            decimal expected = -987.000000000000000000000001m;
            var value = new Decimal256Scalar(expected);
            var bytes = new byte[] {
                240, 216, 255, 79, 199, 211, 79, 103, 166, 90, 79, 108, 131, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255
            };

            Assert.Equal(expected, value.Value);
            Assert.Equal(Decimal256Type.SystemDefault, value.Type);
            Assert.Equal(bytes, value.View().ToArray());
        }
    }
}

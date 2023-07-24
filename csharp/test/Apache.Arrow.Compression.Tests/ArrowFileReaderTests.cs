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

using Apache.Arrow.Ipc;
using System;
using System.Reflection;
using Xunit;

namespace Apache.Arrow.Compression.Tests
{
    public class ArrowFileReaderTests
    {
        [Theory]
        [InlineData("ipc_lz4_compression.arrow")]
        [InlineData("ipc_zstd_compression.arrow")]
        public void CanReadCompressedIpcFile(string fileName)
        {
            var assembly = Assembly.GetExecutingAssembly();
            using var stream = assembly.GetManifestResourceStream($"Apache.Arrow.Compression.Tests.Resources.{fileName}");
            Assert.NotNull(stream);
            var codecFactory = new CompressionCodecFactory();
            using var reader = new ArrowFileReader(stream, codecFactory);

            var batch = reader.ReadNextRecordBatch();

            var intArray = (Int32Array) batch.Column("integers");
            var floatArray = (FloatArray) batch.Column("floats");

            const int numRows = 100;
            Assert.Equal(numRows, intArray.Length);
            Assert.Equal(numRows, floatArray.Length);

            for (var i = 0; i < numRows; ++i)
            {
                Assert.Equal(i, intArray.GetValue(i));
                Assert.True(Math.Abs(floatArray.GetValue(i).Value - 0.1f * i) < 1.0e-6);
            }
        }

        [Fact]
        public void ErrorReadingCompressedFileWithoutCodecFactory()
        {
            var assembly = Assembly.GetExecutingAssembly();
            using var stream = assembly.GetManifestResourceStream("Apache.Arrow.Compression.Tests.Resources.ipc_lz4_compression.arrow");
            Assert.NotNull(stream);
            using var reader = new ArrowFileReader(stream);

            var exception = Assert.Throws<Exception>(() => reader.ReadNextRecordBatch());
            Assert.Contains("no ICompressionCodecFactory has been configured", exception.Message);
        }
    }
}

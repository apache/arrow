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
using System.Runtime.InteropServices;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class CDataInterfaceDataTests
    {
        private IArrowArray GetTestArray()
        {
            var builder = new StringArray.Builder();
            builder.Append("hello");
            builder.Append("world");
            builder.AppendNull();
            builder.Append("foo");
            builder.Append("bar");
            return builder.Build();
        }

        [Fact]
        public unsafe void InitializeArrayZeroed()
        {
            CArrowArray* cArray = CArrowArray.Create();

            Assert.Equal(0, cArray->length);
            Assert.Equal(0, cArray->null_count);
            Assert.Equal(0, cArray->offset);
            Assert.Equal(0, cArray->n_buffers);
            Assert.Equal(0, cArray->n_children);
            Assert.True(cArray->buffers == null);
            Assert.True(cArray->children == null);
            Assert.True(cArray->dictionary == null);
            Assert.True(cArray->release == default);
            Assert.True(cArray->private_data == null);

            CArrowArray.Free(cArray);
        }

        [Fact]
        public unsafe void CallsReleaseForValid()
        {
            IArrowArray array = GetTestArray();
            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(array, cArray);
            Assert.False(cArray->release == default);
            CArrowArrayImporter.ImportArray(cArray, array.Data.DataType).Dispose();
            Assert.True(cArray->release == default);
            CArrowArray.Free(cArray);
        }

#if NET5_0_OR_GREATER
        [Fact]
        public unsafe void CallsReleaseForInvalid()
        {
            // Make sure we call release callback, even if the imported array is invalid.
            CArrowArray* cArray = CArrowArray.Create();

            bool wasCalled = false;
            var releaseCallback = (CArrowArray* cArray) =>
            {
                wasCalled = true;
                cArray->release = default;
            };
            cArray->release = (delegate* unmanaged<CArrowArray*, void>)Marshal.GetFunctionPointerForDelegate(
                releaseCallback);

            Assert.Throws<InvalidOperationException>(() =>
            {
                CArrowArrayImporter.ImportArray(cArray, GetTestArray().Data.DataType);
            });
            Assert.True(wasCalled);

            CArrowArray.Free(cArray);

            GC.KeepAlive(releaseCallback);
        }
#endif

        [Fact]
        public unsafe void RoundTripInt32ArrayWithOffset()
        {
            Int32Array array = new Int32Array.Builder()
                .AppendRange(new[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 })
                .Build();
            IArrowArray sliced = array.Slice(2, 6);
            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(sliced, cArray);
            using (var importedSlice = (Int32Array)CArrowArrayImporter.ImportArray(cArray, array.Data.DataType))
            {
                Assert.Equal(6, importedSlice.Length);
                Assert.Equal(2, importedSlice.Offset);
                Assert.Equal(2, importedSlice.GetValue(0));
            }
            CArrowArray.Free(cArray);
        }

        [Fact]
        public unsafe void ExportRecordBatch_LargerThan2GB_Succeeds()
        {
            RecordBatch GetTestRecordBatch()
            {
                const int rows = 50_000;
                var doubles = new double[rows];
                for (var i = 0; i < rows; ++i)
                {
                    doubles[i] = i * 1.1;
                }

                var batchBuilder = new RecordBatch.Builder();
                for (var i = 0; i < 10_000; i++)
                {
                    batchBuilder.Append($"doubles{i}", true, ab => ab.Double(b => b.Append(doubles)));
                }

                return batchBuilder.Build();
            }

            RecordBatch batch = GetTestRecordBatch();

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportRecordBatch(batch, cArray);

            CArrowArray.Free(cArray);
        }
    }
}

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
            Assert.True(cArray->release == null);
            Assert.True(cArray->private_data == null);

            CArrowArray.Free(cArray);
        }

        [Fact]
        public unsafe void CallsReleaseForValid()
        {
            IArrowArray array = GetTestArray();
            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(array, cArray);
            Assert.False(cArray->release == null);
            CArrowArrayImporter.ImportArray(cArray, array.Data.DataType).Dispose();
            Assert.True(cArray->release == null);
            CArrowArray.Free(cArray);
        }

        [Fact]
        public unsafe void CallsReleaseForInvalid()
        {
            // Make sure we call release callback, even if the imported array is invalid.
            CArrowArray* cArray = CArrowArray.Create();

            bool wasCalled = false;
            var releaseCallback = (CArrowArray* cArray) =>
            {
                wasCalled = true;
                cArray->release = null;
            };
            cArray->release = (delegate* unmanaged[Stdcall]<CArrowArray*, void>)Marshal.GetFunctionPointerForDelegate(
                releaseCallback);

            Assert.Throws<InvalidOperationException>(() =>
            {
                CArrowArrayImporter.ImportArray(cArray, GetTestArray().Data.DataType);
            });
            Assert.True(wasCalled);
            CArrowArray.Free(cArray);

            GC.KeepAlive(releaseCallback);
        }
    }
}

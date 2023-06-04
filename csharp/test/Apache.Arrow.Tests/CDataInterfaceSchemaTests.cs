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
    public class CDataSchemaTest
    {
        [Fact]
        public unsafe void InitializeZeroed()
        {
            CArrowSchema* cSchema = CArrowSchema.Create();

            Assert.True(cSchema->format == null);
            Assert.True(cSchema->name == null);
            Assert.True(cSchema->metadata == null);
            Assert.Equal(0, cSchema->flags);
            Assert.Equal(0, cSchema->n_children);
            Assert.True(cSchema->children == null);
            Assert.True(cSchema->dictionary == null);
            Assert.True(cSchema->release == null);
            Assert.True(cSchema->private_data == null);

            CArrowSchema.Free(cSchema);
        }

        [Fact]
        public unsafe void FlagsSet()
        {
            // Non-nullable field
            {
                var nonNullField = new Field("non_null", Int32Type.Default, false);
                CArrowSchema* cSchema = CArrowSchema.Create();
                CArrowSchemaExporter.ExportField(nonNullField, cSchema);
                Assert.False(cSchema->GetFlag(CArrowSchema.ArrowFlagNullable));
                CArrowSchema.Free(cSchema);
            }

            // Nullable field
            {
                var nullableField = new Field("nullable", Int32Type.Default, true);
                CArrowSchema* cSchema = CArrowSchema.Create();
                CArrowSchemaExporter.ExportField(nullableField, cSchema);
                Assert.True(cSchema->GetFlag(CArrowSchema.ArrowFlagNullable));
                CArrowSchema.Free(cSchema);
            }

            // dictionary ordered
            {
                var orderedDictionary = new DictionaryType(Int32Type.Default, StringType.Default, true);
                CArrowSchema* cSchema = CArrowSchema.Create();
                CArrowSchemaExporter.ExportType(orderedDictionary, cSchema);
                Assert.True(cSchema->GetFlag(CArrowSchema.ArrowFlagDictionaryOrdered));
                CArrowSchema.Free(cSchema);
            }

            // dictionary unordered
            {
                var unorderedDictionary = new DictionaryType(Int32Type.Default, StringType.Default, false);
                CArrowSchema* cSchema = CArrowSchema.Create();
                CArrowSchemaExporter.ExportType(unorderedDictionary, cSchema);
                Assert.False(cSchema->GetFlag(CArrowSchema.ArrowFlagDictionaryOrdered));
                CArrowSchema.Free(cSchema);
            }
        }

        [Fact]
        public unsafe void CallsReleaseForValid()
        {
            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportType(Int32Type.Default, cSchema);
            Assert.False(cSchema->release == null);
            CArrowSchemaImporter.ImportType(cSchema);
            Assert.True(cSchema->release == null);
            CArrowSchema.Free(cSchema);
        }

        [Fact]
        public unsafe void CallsReleaseForInvalid()
        {
            // Make sure we call release callback, even if the imported schema
            // is invalid.
            CArrowSchema* cSchema = CArrowSchema.Create();

            bool wasCalled = false;
            var releaseCallback = (CArrowSchema* cSchema) =>
            {
                wasCalled = true;
                cSchema->release = null;
            };
            cSchema->release = (delegate* unmanaged[Stdcall]<CArrowSchema*, void>)Marshal.GetFunctionPointerForDelegate(
                releaseCallback);

            Assert.Throws<NullReferenceException>(() =>
            {
                CArrowSchemaImporter.ImportType(cSchema);
            });
            Assert.True(wasCalled);
            CArrowSchema.Free(cSchema);

            GC.KeepAlive(releaseCallback);
        }
    }
}

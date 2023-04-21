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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using Python.Runtime;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class CDataSchemaPythonTest
    {
        public CDataSchemaPythonTest()
        {
            bool inCIJob = Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true";
            bool inVerificationJob = Environment.GetEnvironmentVariable("TEST_CSHARP") == "1";
            bool pythonSet = Environment.GetEnvironmentVariable("PYTHONNET_PYDLL") != null;
            // We only skip if this is not in CI
            if (inCIJob && !inVerificationJob && !pythonSet)
            {
                throw new Exception("PYTHONNET_PYDLL not set; skipping C Data Interface tests.");
            }
            else
            {
                Skip.If(!pythonSet, "PYTHONNET_PYDLL not set; skipping C Data Interface tests.");
            }


            PythonEngine.Initialize();

            if (System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
                !PythonEngine.PythonPath.Contains("dlls", StringComparison.OrdinalIgnoreCase))
            {
                dynamic sys = Py.Import("sys");
                sys.path.append(Path.Combine(Path.GetDirectoryName(Environment.GetEnvironmentVariable("PYTHONNET_PYDLL")), "DLLs"));
            }
        }

        private static Schema GetTestSchema()
        {
            using (Py.GIL())
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("null").DataType(NullType.Default).Nullable(true))
                    .Field(f => f.Name("bool").DataType(BooleanType.Default).Nullable(true))
                    .Field(f => f.Name("i8").DataType(Int8Type.Default).Nullable(true))
                    .Field(f => f.Name("u8").DataType(UInt8Type.Default).Nullable(true))
                    .Field(f => f.Name("i16").DataType(Int16Type.Default).Nullable(true))
                    .Field(f => f.Name("u16").DataType(UInt16Type.Default).Nullable(true))
                    .Field(f => f.Name("i32").DataType(Int32Type.Default).Nullable(true))
                    .Field(f => f.Name("u32").DataType(UInt32Type.Default).Nullable(true))
                    .Field(f => f.Name("i64").DataType(Int64Type.Default).Nullable(true))
                    .Field(f => f.Name("u64").DataType(UInt64Type.Default).Nullable(true))

                    .Field(f => f.Name("f16").DataType(HalfFloatType.Default).Nullable(true))
                    .Field(f => f.Name("f32").DataType(FloatType.Default).Nullable(true))
                    .Field(f => f.Name("f64").DataType(DoubleType.Default).Nullable(true))

                    .Field(f => f.Name("decimal128_19_3").DataType(new Decimal128Type(19, 3)).Nullable(true))
                    .Field(f => f.Name("decimal256_19_3").DataType(new Decimal256Type(19, 3)).Nullable(true))
                    .Field(f => f.Name("decimal256_40_2").DataType(new Decimal256Type(40, 2)).Nullable(false))

                    .Field(f => f.Name("binary").DataType(BinaryType.Default).Nullable(false))
                    .Field(f => f.Name("string").DataType(StringType.Default).Nullable(false))
                    .Field(f => f.Name("fw_binary_10").DataType(new FixedSizeBinaryType(10)).Nullable(false))

                    .Field(f => f.Name("date32").DataType(Date32Type.Default).Nullable(false))
                    .Field(f => f.Name("date64").DataType(Date64Type.Default).Nullable(false))
                    .Field(f => f.Name("time32_s").DataType(new Time32Type(TimeUnit.Second)).Nullable(false))
                    .Field(f => f.Name("time32_ms").DataType(new Time32Type(TimeUnit.Millisecond)).Nullable(false))
                    .Field(f => f.Name("time64_us").DataType(new Time64Type(TimeUnit.Microsecond)).Nullable(false))
                    .Field(f => f.Name("time64_ns").DataType(new Time64Type(TimeUnit.Nanosecond)).Nullable(false))

                    .Field(f => f.Name("timestamp_ns").DataType(new TimestampType(TimeUnit.Nanosecond, "")).Nullable(false))
                    .Field(f => f.Name("timestamp_us").DataType(new TimestampType(TimeUnit.Microsecond, "")).Nullable(false))
                    .Field(f => f.Name("timestamp_us_paris").DataType(new TimestampType(TimeUnit.Microsecond, "Europe/Paris")).Nullable(true))

                    .Field(f => f.Name("list_string").DataType(new ListType(StringType.Default)).Nullable(false))
                    .Field(f => f.Name("list_list_i32").DataType(new ListType(new ListType(Int32Type.Default))).Nullable(false))

                    .Field(f => f.Name("dict_string").DataType(new DictionaryType(Int32Type.Default, StringType.Default, false)).Nullable(false))
                    .Field(f => f.Name("dict_string_ordered").DataType(new DictionaryType(Int32Type.Default, StringType.Default, true)).Nullable(false))
                    .Field(f => f.Name("list_dict_string").DataType(new ListType(new DictionaryType(Int32Type.Default, StringType.Default, false))).Nullable(false))

                    // Checking wider characters.
                    .Field(f => f.Name("hello 你好 😄").DataType(BooleanType.Default).Nullable(true))

                    .Build();
                return schema;
            }
        }

        private static IEnumerable<dynamic> GetPythonFields()
        {
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                yield return pa.field("null", pa.GetAttr("null").Invoke(), true);
                yield return pa.field("bool", pa.bool_(), true);
                yield return pa.field("i8", pa.int8(), true);
                yield return pa.field("u8", pa.uint8(), true);
                yield return pa.field("i16", pa.int16(), true);
                yield return pa.field("u16", pa.uint16(), true);
                yield return pa.field("i32", pa.int32(), true);
                yield return pa.field("u32", pa.uint32(), true);
                yield return pa.field("i64", pa.int64(), true);
                yield return pa.field("u64", pa.uint64(), true);

                yield return pa.field("f16", pa.float16(), true);
                yield return pa.field("f32", pa.float32(), true);
                yield return pa.field("f64", pa.float64(), true);

                yield return pa.field("decimal128_19_3", pa.decimal128(19, 3), true);
                yield return pa.field("decimal256_19_3", pa.decimal256(19, 3), true);
                yield return pa.field("decimal256_40_2", pa.decimal256(40, 2), false);

                yield return pa.field("binary", pa.binary(), false);
                yield return pa.field("string", pa.utf8(), false);
                yield return pa.field("fw_binary_10", pa.binary(10), false);

                yield return pa.field("date32", pa.date32(), false);
                yield return pa.field("date64", pa.date64(), false);
                yield return pa.field("time32_s", pa.time32("s"), false);
                yield return pa.field("time32_ms", pa.time32("ms"), false);
                yield return pa.field("time64_us", pa.time64("us"), false);
                yield return pa.field("time64_ns", pa.time64("ns"), false);

                yield return pa.field("timestamp_ns", pa.timestamp("ns"), false);
                yield return pa.field("timestamp_us", pa.timestamp("us"), false);
                yield return pa.field("timestamp_us_paris", pa.timestamp("us", "Europe/Paris"), true);

                yield return pa.field("list_string", pa.list_(pa.utf8()), false);
                yield return pa.field("list_list_i32", pa.list_(pa.list_(pa.int32())), false);

                yield return pa.field("dict_string", pa.dictionary(pa.int32(), pa.utf8(), false), false);
                yield return pa.field("dict_string_ordered", pa.dictionary(pa.int32(), pa.utf8(), true), false);
                yield return pa.field("list_dict_string", pa.list_(pa.dictionary(pa.int32(), pa.utf8(), false)), false);

                yield return pa.field("hello 你好 😄", pa.bool_(), true);
            }
        }

        private static dynamic GetPythonSchema()
        {
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                return pa.schema(GetPythonFields().ToList());
            }
        }

        // Schemas created in Python, used in CSharp
        [SkippableFact]
        public unsafe void ImportType()
        {
            Schema schema = GetTestSchema();
            IEnumerable<dynamic> pyFields = GetPythonFields();

            foreach ((Field field, dynamic pyField) in schema.FieldsList
                .Zip(pyFields))
            {
                CArrowSchema* cSchema = CArrowSchema.Create();

                using (Py.GIL())
                {
                    dynamic pyDatatype = pyField.type;
                    // Python expects the pointer as an integer
                    long longPtr = ((IntPtr)cSchema).ToInt64();
                    pyDatatype._export_to_c(longPtr);
                }

                var dataTypeComparer = new ArrayTypeComparer(field.DataType);
                ArrowType importedType = CArrowSchemaImporter.ImportType(cSchema);
                dataTypeComparer.Visit(importedType);

                if (importedType is DictionaryType importedDictType)
                {
                    Assert.Equal(((DictionaryType)field.DataType).Ordered, importedDictType.Ordered);
                }

                // Since we allocated, we are responsible for freeing the pointer.
                CArrowSchema.Free(cSchema);
            }
        }

        [SkippableFact]
        public unsafe void ImportField()
        {
            Schema schema = GetTestSchema();
            IEnumerable<dynamic> pyFields = GetPythonFields();

            foreach ((Field field, dynamic pyField) in schema.FieldsList
                .Zip(pyFields))
            {
                CArrowSchema* cSchema = CArrowSchema.Create();

                using (Py.GIL())
                {
                    long longPtr = ((IntPtr)cSchema).ToInt64();
                    pyField._export_to_c(longPtr);
                }

                Field importedField = CArrowSchemaImporter.ImportField(cSchema);
                FieldComparer.Compare(field, importedField);

                // Since we allocated, we are responsible for freeing the pointer.
                CArrowSchema.Free(cSchema);
            }
        }

        [SkippableFact]
        public unsafe void ImportSchema()
        {
            Schema schema = GetTestSchema();
            dynamic pySchema = GetPythonSchema();

            CArrowSchema* cSchema = CArrowSchema.Create();

            using (Py.GIL())
            {
                long longPtr = ((IntPtr)cSchema).ToInt64();
                pySchema._export_to_c(longPtr);
            }

            Schema importedSchema = CArrowSchemaImporter.ImportSchema(cSchema);
            SchemaComparer.Compare(schema, importedSchema);

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowSchema.Free(cSchema);
        }


        // Schemas created in CSharp, exported to Python
        [SkippableFact]
        public unsafe void ExportType()
        {
            Schema schema = GetTestSchema();
            IEnumerable<dynamic> pyFields = GetPythonFields();

            foreach ((Field field, dynamic pyField) in schema.FieldsList
                .Zip(pyFields))
            {
                IArrowType datatype = field.DataType;
                CArrowSchema* cSchema = CArrowSchema.Create();
                CArrowSchemaExporter.ExportType(datatype, cSchema);

                // For Python, we need to provide the pointer
                long longPtr = ((IntPtr)cSchema).ToInt64();

                using (Py.GIL())
                {
                    dynamic pa = Py.Import("pyarrow");
                    dynamic expectedPyType = pyField.type;
                    dynamic exportedPyType = pa.DataType._import_from_c(longPtr);
                    Assert.True(exportedPyType == expectedPyType);

                    if (pa.types.is_dictionary(exportedPyType))
                    {
                        Assert.Equal(expectedPyType.ordered, exportedPyType.ordered);
                    }
                }

                // Python should have called release once `exportedPyType` went out-of-scope.
                Assert.True(cSchema->release == null);
                Assert.True(cSchema->format == null);
                Assert.Equal(0, cSchema->flags);
                Assert.Equal(0, cSchema->n_children);
                Assert.True(cSchema->dictionary == null);

                // Since we allocated, we are responsible for freeing the pointer.
                CArrowSchema.Free(cSchema);
            }
        }

        [SkippableFact]
        public unsafe void ExportField()
        {
            Schema schema = GetTestSchema();
            IEnumerable<dynamic> pyFields = GetPythonFields();

            foreach ((Field field, dynamic pyField) in schema.FieldsList
                .Zip(pyFields))
            {
                CArrowSchema* cSchema = CArrowSchema.Create();
                CArrowSchemaExporter.ExportField(field, cSchema);

                // For Python, we need to provide the pointer
                long longPtr = ((IntPtr)cSchema).ToInt64();

                using (Py.GIL())
                {
                    dynamic pa = Py.Import("pyarrow");
                    dynamic exportedPyField = pa.Field._import_from_c(longPtr);
                    Assert.True(exportedPyField == pyField);
                }

                // Python should have called release once `exportedPyField` went out-of-scope.
                Assert.True(cSchema->name == null);
                Assert.True(cSchema->release == null);
                Assert.True(cSchema->format == null);

                // Since we allocated, we are responsible for freeing the pointer.
                CArrowSchema.Free(cSchema);
            }
        }

        [SkippableFact]
        public unsafe void ExportSchema()
        {
            Schema schema = GetTestSchema();
            dynamic pySchema = GetPythonSchema();

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(schema, cSchema);

            // For Python, we need to provide the pointer
            long longPtr = ((IntPtr)cSchema).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPySchema = pa.Schema._import_from_c(longPtr);
                Assert.True(exportedPySchema == pySchema);
            }

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowSchema.Free(cSchema);
        }
    }
}

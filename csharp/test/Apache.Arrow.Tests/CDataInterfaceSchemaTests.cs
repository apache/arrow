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
using System.Linq;
using System.Runtime.InteropServices;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using Python.Runtime;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class CDataSchemaTest
    {
        public CDataSchemaTest()
        {
            PythonEngine.Initialize();
        }

        public static Schema GetTestSchema()
        {
            // TODO: Add more types
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

                    .Field(f => f.Name("binary").DataType(BinaryType.Default).Nullable(false))
                    .Field(f => f.Name("string").DataType(StringType.Default).Nullable(false))

                    .Field(f => f.Name("date32").DataType(Date32Type.Default).Nullable(false))
                    .Field(f => f.Name("date64").DataType(Date64Type.Default).Nullable(false))
                    .Field(f => f.Name("time32_s").DataType(new Time32Type(TimeUnit.Second)).Nullable(false))
                    .Field(f => f.Name("time32_ms").DataType(new Time32Type(TimeUnit.Millisecond)).Nullable(false))
                    .Field(f => f.Name("time64_us").DataType(new Time64Type(TimeUnit.Microsecond)).Nullable(false))
                    .Field(f => f.Name("time64_ns").DataType(new Time64Type(TimeUnit.Nanosecond)).Nullable(false))

                    .Field(f => f.Name("list_string").DataType(new ListType(StringType.Default)).Nullable(false))
                    .Field(f => f.Name("list_list_i32").DataType(new ListType(new ListType(Int32Type.Default))).Nullable(false))

                    .Field(f => f.Name("dict_string").DataType(new DictionaryType(Int32Type.Default, StringType.Default, false)).Nullable(false))
                    .Field(f => f.Name("dict_string_ordered").DataType(new DictionaryType(Int32Type.Default, StringType.Default, true)).Nullable(false))
                    .Field(f => f.Name("list_dict_string").DataType(new ListType(new DictionaryType(Int32Type.Default, StringType.Default, false))).Nullable(false))

                    .Build();
                return schema;
            }
        }

        public static IEnumerable<dynamic> GetPythonFields()
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

                yield return pa.field("binary", pa.binary(), false);
                yield return pa.field("string", pa.utf8(), false);

                yield return pa.field("date32", pa.date32(), false);
                yield return pa.field("date64", pa.date64(), false);
                yield return pa.field("time32_s", pa.time32("s"), false);
                yield return pa.field("time32_ms", pa.time32("ms"), false);
                yield return pa.field("time64_us", pa.time64("us"), false);
                yield return pa.field("time64_ns", pa.time64("ns"), false);

                yield return pa.field("list_string", pa.list_(pa.utf8()), false);
                yield return pa.field("list_list_i32", pa.list_(pa.list_(pa.int32())), false);

                yield return pa.field("dict_string", pa.dictionary(pa.int32(), pa.utf8(), false), false);
                yield return pa.field("dict_string_ordered", pa.dictionary(pa.int32(), pa.utf8(), true), false);
                yield return pa.field("list_dict_string", pa.list_(pa.dictionary(pa.int32(), pa.utf8(), false)), false);
            }
        }

        public static dynamic GetPythonSchema()
        {
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                return pa.schema(GetPythonFields().ToList());
            }
        }

        // Schemas created in Python, used in CSharp
        [Fact]
        public void ImportType()
        {
            PythonEngine.Initialize();
            Schema schema = GetTestSchema();
            IEnumerable<dynamic> pyFields = GetPythonFields();

            foreach ((Field field, dynamic pyField) in schema.Fields.Values.AsEnumerable()
                .Zip(pyFields))
            {
                var cSchema = new CArrowSchema();
                IntPtr importedPtr = cSchema.AllocateAsPtr();

                using (Py.GIL())
                {
                    dynamic py_datatype = pyField.type;
                    py_datatype._export_to_c(importedPtr.ToInt64());
                }

                var dataTypeComparer = new ArrayTypeComparer(field.DataType);
                var importedType = new ImportedArrowSchema(importedPtr);
                dataTypeComparer.Visit(importedType.GetAsType());

                // Since we allocated, we are responsible for freeing the pointer.
                CArrowSchema.FreePtr(importedPtr);
            }
        }

        [Fact]
        public void ImportField()
        {
            PythonEngine.Initialize();
            Schema schema = GetTestSchema();
            IEnumerable<dynamic> pyFields = GetPythonFields();

            foreach ((Field field, dynamic pyField) in schema.Fields.Values.AsEnumerable()
                .Zip(pyFields))
            {
                var cSchema = new CArrowSchema();
                IntPtr importedPtr = cSchema.AllocateAsPtr();

                using (Py.GIL())
                {
                    pyField._export_to_c(importedPtr.ToInt64());
                }

                var importedField = new ImportedArrowSchema(importedPtr);
                FieldComparer.Compare(field, importedField.GetAsField());

                // Since we allocated, we are responsible for freeing the pointer.
                CArrowSchema.FreePtr(importedPtr);
            }
        }

        [Fact]
        public void ImportSchema()
        {
            PythonEngine.Initialize();
            Schema schema = GetTestSchema();
            dynamic pySchema = GetPythonSchema();

            var cSchema = new CArrowSchema();
            IntPtr importedPtr = cSchema.AllocateAsPtr();

            using (Py.GIL())
            {
                pySchema._export_to_c(importedPtr.ToInt64());
            }

            var importedField = new ImportedArrowSchema(importedPtr);
            SchemaComparer.Compare(schema, importedField.GetAsSchema());

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowSchema.FreePtr(importedPtr);
        }


        // Schemas created in CSharp, exported to Python
        [Fact]
        public void ExportType()
        {
            PythonEngine.Initialize();
            Schema schema = GetTestSchema();
            IEnumerable<dynamic> pyFields = GetPythonFields();

            foreach ((Field field, dynamic pyField) in schema.Fields.Values.AsEnumerable()
                .Zip(pyFields))
            {
                IArrowType datatype = field.DataType;
                var exportedType = new CArrowSchema();
                CArrowSchema.ExportDataType(datatype, out exportedType);

                // For Python, we need to provide the pointer
                IntPtr exportedPtr = exportedType.AllocateAsPtr();

                using (Py.GIL())
                {
                    dynamic pa = Py.Import("pyarrow");
                    dynamic expectedPyType = pyField.type;
                    dynamic exportedPyType = pa.DataType._import_from_c(exportedPtr.ToInt64());
                    Assert.True(exportedPyType == expectedPyType);
                }

                // Python should have called release once `exported_py_type` went out-of-scope.
                var cSchema = Marshal.PtrToStructure<CArrowSchema>(exportedPtr);
                Assert.Null(cSchema.release);
                Assert.Null(cSchema.format);
                Assert.Equal(0, cSchema.flags);
                Assert.Equal(0, cSchema.n_children);
                Assert.Equal(IntPtr.Zero, cSchema.dictionary);

                // Since we allocated, we are responsible for freeing the pointer.
                CArrowSchema.FreePtr(exportedPtr);
            }
        }

        [Fact]
        public void ExportField()
        {
            PythonEngine.Initialize();
            Schema schema = GetTestSchema();
            IEnumerable<dynamic> pyFields = GetPythonFields();

            foreach ((Field field, dynamic pyField) in schema.Fields.Values.AsEnumerable()
                .Zip(pyFields))
            {
                var exportedField = new CArrowSchema();
                CArrowSchema.ExportField(field, out exportedField);

                // For Python, we need to provide the pointer
                IntPtr exportedPtr = exportedField.AllocateAsPtr();

                using (Py.GIL())
                {
                    dynamic pa = Py.Import("pyarrow");
                    dynamic exportedPyField = pa.Field._import_from_c(exportedPtr.ToInt64());
                    Assert.True(exportedPyField == pyField);
                }

                // Python should have called release once `exported_py_type` went out-of-scope.
                var ffiSchema = Marshal.PtrToStructure<CArrowSchema>(exportedPtr);
                Assert.Null(ffiSchema.name);
                Assert.Null(ffiSchema.release);
                Assert.Null(ffiSchema.format);

                // Since we allocated, we are responsible for freeing the pointer.
                CArrowSchema.FreePtr(exportedPtr);
            }
        }

        [Fact]
        public void ExportSchema()
        {
            PythonEngine.Initialize();
            Schema schema = GetTestSchema();
            dynamic pySchema = GetPythonSchema();

            var exportedSchema = new CArrowSchema();
            CArrowSchema.ExportSchema(schema, out exportedSchema);

            // For Python, we need to provide the pointer
            IntPtr exportedPtr = exportedSchema.AllocateAsPtr();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPySchema = pa.Schema._import_from_c(exportedPtr.ToInt64());
                Assert.True(exportedPySchema == pySchema);
            }

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowSchema.FreePtr(exportedPtr);
        }
    }
}

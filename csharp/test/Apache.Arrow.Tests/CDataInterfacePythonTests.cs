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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
using Python.Runtime;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class CDataSchemaPythonTest : IClassFixture<CDataSchemaPythonTest.PythonNet>
    {
        class PythonNet : IDisposable
        {
            public PythonNet()
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

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
                    PythonEngine.PythonPath.IndexOf("dlls", StringComparison.OrdinalIgnoreCase) < 0)
                {
                    dynamic sys = Py.Import("sys");
                    sys.path.append(Path.Combine(Path.GetDirectoryName(Environment.GetEnvironmentVariable("PYTHONNET_PYDLL")), "DLLs"));
                }
            }

            public void Dispose()
            {
                PythonEngine.Shutdown();
            }
        }

        private static Schema GetTestSchema()
        {
            using (Py.GIL())
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("null").DataType(NullType.Default).Nullable(true).Metadata("k0", "v0"))
                    .Field(f => f.Name("bool").DataType(BooleanType.Default).Nullable(true))
                    .Field(f => f.Name("i8").DataType(Int8Type.Default).Nullable(true))
                    .Field(f => f.Name("u8").DataType(UInt8Type.Default).Nullable(true))
                    .Field(f => f.Name("i16").DataType(Int16Type.Default).Nullable(true))
                    .Field(f => f.Name("u16").DataType(UInt16Type.Default).Nullable(true))
                    .Field(f => f.Name("i32").DataType(Int32Type.Default).Nullable(true))
                    .Field(f => f.Name("u32").DataType(UInt32Type.Default).Nullable(true))
                    .Field(f => f.Name("i64").DataType(Int64Type.Default).Nullable(true))
                    .Field(f => f.Name("u64").DataType(UInt64Type.Default).Nullable(true))

                    .Field(f => f.Name("f16").DataType(HalfFloatType.Default).Nullable(true).Metadata("k1a", "").Metadata("k1b", "断箭"))
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

                    .Field(f => f.Name("timestamp_ns").DataType(new TimestampType(TimeUnit.Nanosecond, (string) null)).Nullable(false))
                    .Field(f => f.Name("timestamp_us").DataType(new TimestampType(TimeUnit.Microsecond, (string) null)).Nullable(false))
                    .Field(f => f.Name("timestamp_us_paris").DataType(new TimestampType(TimeUnit.Microsecond, "Europe/Paris")).Nullable(true))

                    .Field(f => f.Name("list_string").DataType(new ListType(StringType.Default)).Nullable(false))
                    .Field(f => f.Name("list_list_i32").DataType(new ListType(new ListType(Int32Type.Default))).Nullable(false))

                    .Field(f => f.Name("fixed_length_list_i64").DataType(new FixedSizeListType(Int64Type.Default, 10)).Nullable(true))

                    .Field(f => f.Name("dict_string").DataType(new DictionaryType(Int32Type.Default, StringType.Default, false)).Nullable(false))
                    .Field(f => f.Name("dict_string_ordered").DataType(new DictionaryType(Int32Type.Default, StringType.Default, true)).Nullable(false))
                    .Field(f => f.Name("list_dict_string").DataType(new ListType(new DictionaryType(Int32Type.Default, StringType.Default, false))).Nullable(false))

                    .Field(f => f.Name("dense_union").DataType(new UnionType(new[] { new Field("i64", Int64Type.Default, false), new Field("f32", FloatType.Default, true), }, new[] { 0, 1 }, UnionMode.Dense)))
                    .Field(f => f.Name("sparse_union").DataType(new UnionType(new[] { new Field("i32", Int32Type.Default, true), new Field("f64", DoubleType.Default, false), }, new[] { 0, 1 }, UnionMode.Sparse)))

                    .Field(f => f.Name("map").DataType(new MapType(StringType.Default, Int32Type.Default)).Nullable(false))

                    .Field(f => f.Name("duration_s").DataType(DurationType.Second).Nullable(false))
                    .Field(f => f.Name("duration_ms").DataType(DurationType.Millisecond).Nullable(true))
                    .Field(f => f.Name("duration_us").DataType(DurationType.Microsecond).Nullable(false))
                    .Field(f => f.Name("duration_ns").DataType(DurationType.Nanosecond).Nullable(true))

                    .Field(f => f.Name("interval").DataType(IntervalType.FromIntervalUnit(IntervalUnit.MonthDayNanosecond)))

                    // Checking wider characters.
                    .Field(f => f.Name("hello 你好 😄").DataType(BooleanType.Default).Nullable(true))

                    .Metadata("k2a", "v2abc").Metadata("k2b", "v2abc").Metadata("k2c", "v2abc")
                    .Build();
                return schema;
            }
        }

        private static IEnumerable<dynamic> GetPythonFields()
        {
            using (Py.GIL())
            {
                Dictionary<string, string> metadata0 = new Dictionary<string, string> { { "k0", "v0" } };
                Dictionary<string, string> metadata1 = new Dictionary<string, string> { { "k1a", "" }, { "k1b", "断箭" } };

                dynamic pa = Py.Import("pyarrow");
                yield return pa.field("null", pa.GetAttr("null").Invoke(), true).with_metadata(metadata0);
                yield return pa.field("bool", pa.bool_(), true);
                yield return pa.field("i8", pa.int8(), true);
                yield return pa.field("u8", pa.uint8(), true);
                yield return pa.field("i16", pa.int16(), true);
                yield return pa.field("u16", pa.uint16(), true);
                yield return pa.field("i32", pa.int32(), true);
                yield return pa.field("u32", pa.uint32(), true);
                yield return pa.field("i64", pa.int64(), true);
                yield return pa.field("u64", pa.uint64(), true);

                yield return pa.field("f16", pa.float16(), true).with_metadata(metadata1);
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

                yield return pa.field("fixed_length_list_i64", pa.list_(pa.int64(), 10), true);

                yield return pa.field("dict_string", pa.dictionary(pa.int32(), pa.utf8(), false), false);
                yield return pa.field("dict_string_ordered", pa.dictionary(pa.int32(), pa.utf8(), true), false);
                yield return pa.field("list_dict_string", pa.list_(pa.dictionary(pa.int32(), pa.utf8(), false)), false);

                yield return pa.field("dense_union", pa.dense_union(List(pa.field("i64", pa.int64(), false), pa.field("f32", pa.float32(), true))));
                yield return pa.field("sparse_union", pa.sparse_union(List(pa.field("i32", pa.int32(), true), pa.field("f64", pa.float64(), false))));

                yield return pa.field("map", pa.map_(pa.@string(), pa.int32()), false);

                yield return pa.field("duration_s", pa.duration("s"), false);
                yield return pa.field("duration_ms", pa.duration("ms"), true);
                yield return pa.field("duration_us", pa.duration("us"), false);
                yield return pa.field("duration_ns", pa.duration("ns"), true);

                yield return pa.field("interval", pa.month_day_nano_interval());

                yield return pa.field("hello 你好 😄", pa.bool_(), true);
            }
        }

        private static dynamic GetPythonSchema()
        {
            using (Py.GIL())
            {
                Dictionary<string, string> metadata = new Dictionary<string, string> { { "k2a", "v2abc" }, { "k2b", "v2abc" }, { "k2c", "v2abc" } };

                dynamic pa = Py.Import("pyarrow");
                return pa.schema(GetPythonFields().ToList()).with_metadata(metadata);
            }
        }

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

        private dynamic GetPythonArray()
        {
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                return pa.array(new[] { "hello", "world", null, "foo", "bar" });
            }
        }

        private RecordBatch GetTestRecordBatch()
        {
            Field[] fields = new[]
            {
                new Field("col1", Int64Type.Default, true),
                new Field("col2", StringType.Default, true),
                new Field("col3", DoubleType.Default, true),
            };
            return new RecordBatch(
                new Schema(fields, null),
                new IArrowArray[]
                {
                    new Int64Array.Builder().AppendRange(new long[] { 1, 2, 3 }).AppendNull().Append(5).Build(),
                    GetTestArray(),
                    new DoubleArray.Builder().AppendRange(new double[] { 0.0, 1.4, 2.5, 3.6, 4.7 }).Build(),
                },
                5);
        }

        private dynamic GetPythonRecordBatch()
        {
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic table = pa.table(
                    new PyList(new PyObject[]
                    {
                        pa.array(new long?[] { 1, 2, 3, null, 5 }),
                        pa.array(new[] { "hello", "world", null, "foo", "bar" }),
                        pa.array(new[] { 0.0, 1.4, 2.5, 3.6, 4.7 })
                    }),
                    new[] { "col1", "col2", "col3" });

                return table.to_batches()[0];
            }
        }

        private IArrowArrayStream GetTestArrayStream()
        {
            RecordBatch recordBatch = GetTestRecordBatch();
            return new TestArrayStream(recordBatch.Schema, recordBatch);
        }

        private dynamic GetPythonRecordBatchReader()
        {
            dynamic recordBatch = GetPythonRecordBatch();
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                return pa.RecordBatchReader.from_batches(recordBatch.schema, new PyList(new PyObject[] { recordBatch }));
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
                Assert.True(cSchema->release == default);
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
                Assert.True(cSchema->release == default);
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

        [SkippableFact]
        public unsafe void ImportArray()
        {
            CArrowArray* cArray = CArrowArray.Create();
            CArrowSchema* cSchema = CArrowSchema.Create();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic array = pa.array(new[] { "hello", "world", null, "foo", "bar" });

                long arrayPtr = ((IntPtr)cArray).ToInt64();
                long schemaPtr = ((IntPtr)cSchema).ToInt64();
                array._export_to_c(arrayPtr, schemaPtr);
            }

            ArrowType type = CArrowSchemaImporter.ImportType(cSchema);
            StringArray importedArray = (StringArray)CArrowArrayImporter.ImportArray(cArray, type);

            Assert.Equal(5, importedArray.Length);
            Assert.Equal("hello", importedArray.GetString(0));
            Assert.Equal("world", importedArray.GetString(1));
            Assert.Null(importedArray.GetString(2));
            Assert.Equal("foo", importedArray.GetString(3));
            Assert.Equal("bar", importedArray.GetString(4));

            CArrowArray.Free(cArray);
        }

        [SkippableFact]
        public unsafe void ImportRecordBatch()
        {
            CArrowArray* cArray = CArrowArray.Create();
            CArrowSchema* cSchema = CArrowSchema.Create();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic table = pa.table(
                    new PyList(new PyObject[]
                    {
                        pa.array(new object[] { null, null, null, null, null }),
                        pa.array(List(1, 2, 3, null, 5)),
                        pa.array(List("hello", "world", null, "foo", "bar")),
                        pa.array(List(0.0, 1.4, 2.5, 3.6, 4.7)),
                        pa.array(new PyObject[] { List(1, 2), List(3, 4), PyObject.None, PyObject.None, List(5, 4, 3) }),
                        pa.StructArray.from_arrays(
                            List(
                                List(10, 9, null, null, null),
                                List("banana", "apple", "orange", "cherry", "grape"),
                                List(null, 4.3, -9, 123.456, 0)
                            ),
                            new[] { "fld1", "fld2", "fld3" }),
                        pa.DictionaryArray.from_arrays(
                            pa.array(List(1, 0, 1, 1, null)),
                            pa.array(List("foo", "bar"))),
                        pa.FixedSizeListArray.from_arrays(
                            pa.array(List(1, 2, 3, 4, null, 6, 7, null, null, null)),
                            2),
                        pa.UnionArray.from_dense(
                            pa.array(List(0, 1, 1, 0, 0), type: "int8"),
                            pa.array(List(0, 0, 1, 1, 2), type: "int32"),
                            List(
                                pa.array(List(1, 4, null)),
                                pa.array(List("two", "three"))
                            ),
                            /* field name */ List("i32", "s"),
                            /* type codes */ List(3, 2)),
                        pa.MapArray.from_arrays(
                            List(0, 0, 1, 2, 4, 10),
                            pa.array(List("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten")),
                            pa.array(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))),
                        pa.array(List(1234, 2345, 3456, null, 6789), pa.duration("ms")),
                        pa.array(
                            List(Tuple(1, 2, 3), PyObject.None, Tuple(-1, -2, -3), Tuple(10, 0, 0), Tuple(0, 0, 20)),
                            pa.month_day_nano_interval()),
                    }),
                    new[] { "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11", "col12" });

                dynamic batch = table.to_batches()[0];

                long batchPtr = ((IntPtr)cArray).ToInt64();
                long schemaPtr = ((IntPtr)cSchema).ToInt64();
                batch._export_to_c(batchPtr, schemaPtr);
            }

            Schema schema = CArrowSchemaImporter.ImportSchema(cSchema);
            RecordBatch recordBatch = CArrowArrayImporter.ImportRecordBatch(cArray, schema);
            CArrowArray.Free(cArray);

            Assert.Equal(5, recordBatch.Length);

            NullArray col1 = (NullArray)recordBatch.Column("col1");
            Assert.Equal(5, col1.Length);

            Int64Array col2 = (Int64Array)recordBatch.Column("col2");
            Assert.Equal(new long[] { 1, 2, 3, 0, 5 }, col2.Values.ToArray());
            Assert.False(col2.IsValid(3));

            StringArray col3 = (StringArray)recordBatch.Column("col3");
            Assert.Equal(5, col3.Length);
            Assert.Equal("hello", col3.GetString(0));
            Assert.Equal("world", col3.GetString(1));
            Assert.Null(col3.GetString(2));
            Assert.Equal("foo", col3.GetString(3));
            Assert.Equal("bar", col3.GetString(4));

            DoubleArray col4 = (DoubleArray)recordBatch.Column("col4");
            Assert.Equal(new double[] { 0.0, 1.4, 2.5, 3.6, 4.7 }, col4.Values.ToArray());

            ListArray col5 = (ListArray)recordBatch.Column("col5");
            Assert.Equal(new long[] { 1, 2, 3, 4, 5, 4, 3 }, ((Int64Array)col5.Values).Values.ToArray());
            Assert.Equal(new int[] { 0, 2, 4, 4, 4, 7 }, col5.ValueOffsets.ToArray());
            Assert.False(col5.IsValid(2));
            Assert.False(col5.IsValid(3));

            StructArray col6 = (StructArray)recordBatch.Column("col6");
            Assert.Equal(5, col6.Length);
            Int64Array col6a = (Int64Array)col6.Fields[0];
            Assert.Equal(new long[] { 10, 9, 0, 0, 0 }, col6a.Values.ToArray());
            StringArray col6b = (StringArray)col6.Fields[1];
            Assert.Equal("banana", col6b.GetString(0));
            Assert.Equal("apple", col6b.GetString(1));
            Assert.Equal("orange", col6b.GetString(2));
            Assert.Equal("cherry", col6b.GetString(3));
            Assert.Equal("grape", col6b.GetString(4));
            DoubleArray col6c = (DoubleArray)col6.Fields[2];
            Assert.Equal(new double[] { 0, 4.3, -9, 123.456, 0 }, col6c.Values.ToArray());

            DictionaryArray col7 = (DictionaryArray)recordBatch.Column("col7");
            Assert.Equal(5, col7.Length);
            Int64Array col7a = (Int64Array)col7.Indices;
            Assert.Equal(new long[] { 1, 0, 1, 1, 0 }, col7a.Values.ToArray());
            Assert.False(col7a.IsValid(4));
            StringArray col7b = (StringArray)col7.Dictionary;
            Assert.Equal(2, col7b.Length);
            Assert.Equal("foo", col7b.GetString(0));
            Assert.Equal("bar", col7b.GetString(1));

            FixedSizeListArray col8 = (FixedSizeListArray)recordBatch.Column("col8");
            Assert.Equal(5, col8.Length);
            Int64Array col8a = (Int64Array)col8.Values;
            Assert.Equal(new long[] { 1, 2, 3, 4, 0, 6, 7, 0, 0, 0 }, col8a.Values.ToArray());
            Assert.True(col8a.IsValid(3));
            Assert.False(col8a.IsValid(9));

            UnionArray col9 = (UnionArray)recordBatch.Column("col9");
            Assert.Equal(5, col9.Length);
            Assert.True(col9 is DenseUnionArray);

            MapArray col10 = (MapArray)recordBatch.Column("col10");
            Assert.Equal(5, col10.Length);
            Assert.Equal(new int[] { 0, 0, 1, 2, 4, 10}, col10.ValueOffsets.ToArray());
            Assert.Equal(new long?[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, ((Int64Array)col10.Values).ToList().ToArray());

            DurationArray col11 = (DurationArray)recordBatch.Column("col11");
            Assert.Equal(5, col11.Length);

            MonthDayNanosecondIntervalArray col12 = (MonthDayNanosecondIntervalArray)recordBatch.Column("col12");
            Assert.Equal(5, col12.Length);
            Assert.Equal(new MonthDayNanosecondInterval(1, 2, 3), col12.GetValue(0));
            Assert.Null(col12.GetValue(1));
            Assert.Equal(new MonthDayNanosecondInterval(-1, -2, -3), col12.GetValue(2));
            Assert.Equal(new MonthDayNanosecondInterval(10, 0, 0), col12.GetValue(3));
            Assert.Equal(new MonthDayNanosecondInterval(0, 0, 20), col12.GetValue(4));
        }

        [SkippableFact]
        public unsafe void ImportArrayStream()
        {
            CArrowArrayStream* cArrayStream = CArrowArrayStream.Create();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic table = pa.table(
                    new PyList(new PyObject[]
                    {
                        pa.array(new long?[] { 1, 2, 3, null, 5 }),
                        pa.array(new[] { "hello", "world", null, "foo", "bar" }),
                        pa.array(new[] { 0.0, 1.4, 2.5, 3.6, 4.7 })
                    }),
                    new[] { "col1", "col2", "col3" });

                dynamic batchReader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches());

                long streamPtr = ((IntPtr)cArrayStream).ToInt64();
                batchReader._export_to_c(streamPtr);
            }

            IArrowArrayStream stream = CArrowArrayStreamImporter.ImportArrayStream(cArrayStream);
            CArrowArrayStream.Free(cArrayStream);

            var batch1 = stream.ReadNextRecordBatchAsync().Result;
            Assert.Equal(5, batch1.Length);

            Int64Array col1 = (Int64Array)batch1.Column("col1");
            Assert.Equal(5, col1.Length);
            Assert.Equal(1, col1.GetValue(0));
            Assert.Equal(2, col1.GetValue(1));
            Assert.Equal(3, col1.GetValue(2));
            Assert.Null(col1.GetValue(3));
            Assert.Equal(5, col1.GetValue(4));

            StringArray col2 = (StringArray)batch1.Column("col2");
            Assert.Equal(5, col2.Length);
            Assert.Equal("hello", col2.GetString(0));
            Assert.Equal("world", col2.GetString(1));
            Assert.Null(col2.GetString(2));
            Assert.Equal("foo", col2.GetString(3));
            Assert.Equal("bar", col2.GetString(4));

            DoubleArray col3 = (DoubleArray)batch1.Column("col3");
            Assert.Equal(new double[] { 0.0, 1.4, 2.5, 3.6, 4.7 }, col3.Values.ToArray());

            var batch2 = stream.ReadNextRecordBatchAsync().Result;
            Assert.Null(batch2);
        }

        [SkippableFact]
        public unsafe void ExportArray()
        {
            IArrowArray array = GetTestArray();
            dynamic pyArray = GetPythonArray();

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(array, cArray);

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportType(array.Data.DataType, cSchema);

            // For Python, we need to provide the pointers
            long arrayPtr = ((IntPtr)cArray).ToInt64();
            long schemaPtr = ((IntPtr)cSchema).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPyArray = pa.Array._import_from_c(arrayPtr, schemaPtr);
                Assert.True(exportedPyArray == pyArray);
            }

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowArray.Free(cArray);
            CArrowSchema.Free(cSchema);
        }

        [SkippableFact]
        public unsafe void ExportBatch()
        {
            RecordBatch batch = GetTestRecordBatch();
            dynamic pyBatch = GetPythonRecordBatch();

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportRecordBatch(batch, cArray);

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(batch.Schema, cSchema);

            // For Python, we need to provide the pointers
            long arrayPtr = ((IntPtr)cArray).ToInt64();
            long schemaPtr = ((IntPtr)cSchema).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPyArray = pa.RecordBatch._import_from_c(arrayPtr, schemaPtr);
                Assert.True(exportedPyArray == pyBatch);
            }

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowArray.Free(cArray);
            CArrowSchema.Free(cSchema);
        }

        [SkippableFact]
        public unsafe void RoundTripTestBatch()
        {
            // TODO: Enable these once this the version of pyarrow referenced during testing supports them
            HashSet<ArrowTypeId> unsupported = new HashSet<ArrowTypeId> { ArrowTypeId.ListView, ArrowTypeId.BinaryView, ArrowTypeId.StringView };
            RecordBatch batch1 = TestData.CreateSampleRecordBatch(4, excludedTypes: unsupported);
            RecordBatch batch2 = batch1.Clone();

            CArrowArray* cExportArray = CArrowArray.Create();
            CArrowArrayExporter.ExportRecordBatch(batch1, cExportArray);

            CArrowSchema* cExportSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(batch1.Schema, cExportSchema);

            CArrowArray* cImportArray = CArrowArray.Create();
            CArrowSchema* cImportSchema = CArrowSchema.Create();

            // For Python, we need to provide the pointers
            long exportArrayPtr = ((IntPtr)cExportArray).ToInt64();
            long exportSchemaPtr = ((IntPtr)cExportSchema).ToInt64();
            long importArrayPtr = ((IntPtr)cImportArray).ToInt64();
            long importSchemaPtr = ((IntPtr)cImportSchema).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPyArray = pa.RecordBatch._import_from_c(exportArrayPtr, exportSchemaPtr);
                exportedPyArray._export_to_c(importArrayPtr, importSchemaPtr);
            }

            Schema schema = CArrowSchemaImporter.ImportSchema(cImportSchema);
            RecordBatch importedBatch = CArrowArrayImporter.ImportRecordBatch(cImportArray, schema);

            ArrowReaderVerifier.CompareBatches(batch2, importedBatch, strictCompare: false); // Non-strict because span lengths won't match.

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowArray.Free(cExportArray);
            CArrowSchema.Free(cExportSchema);
            CArrowArray.Free(cImportArray);
            CArrowSchema.Free(cImportSchema);
        }

        [SkippableFact]
        public unsafe void ExportBatchReader()
        {
            RecordBatch batch = GetTestRecordBatch();
            dynamic pyBatch = GetPythonRecordBatch();

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportRecordBatch(batch, cArray);

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(batch.Schema, cSchema);

            // For Python, we need to provide the pointers
            long arrayPtr = ((IntPtr)cArray).ToInt64();
            long schemaPtr = ((IntPtr)cSchema).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPyArray = pa.RecordBatch._import_from_c(arrayPtr, schemaPtr);
                Assert.True(exportedPyArray == pyBatch);
            }

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowArray.Free(cArray);
            CArrowSchema.Free(cSchema);
        }

        [SkippableFact]
        public unsafe void ExportArrayStream()
        {
            IArrowArrayStream arrayStream = GetTestArrayStream();
            dynamic pyRecordBatchReader = GetPythonRecordBatchReader();

            CArrowArrayStream* cArrayStream = CArrowArrayStream.Create();
            CArrowArrayStreamExporter.ExportArrayStream(arrayStream, cArrayStream);

            // For Python, we need to provide the pointers
            long arrayStreamPtr = ((IntPtr)cArrayStream).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPyReader = pa.RecordBatchReader._import_from_c(arrayStreamPtr);
                Assert.True(exportedPyReader.read_all() == pyRecordBatchReader.read_all());
            }

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowArrayStream.Free(cArrayStream);
        }

        [SkippableFact]
        public unsafe void ImportRecordBatchFromBuffer()
        {
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic batch = pa.record_batch(
                    new PyList(new PyObject[]
                    {
                        pa.array(new long?[] { 1, 2, 3, null, 5 }),
                        pa.array(new[] { "hello", "world", null, "foo", "bar" }),
                        pa.array(new[] { 0.0, 1.4, 2.5, 3.6, 4.7 }),
                        pa.array(new bool?[] { true, false, null, false, true }),
                    }),
                    new[] { null, "", "column", "column" });

                dynamic sink = pa.BufferOutputStream();
                dynamic writer = pa.ipc.new_stream(sink, batch.schema);
                writer.write_batch(batch);
                ((IDisposable)writer).Dispose();

                dynamic buf = sink.getvalue();
                // buf.address, buf.size

                IntPtr address = (IntPtr)(long)buf.address;
                int size = buf.size;
                byte[] buffer = new byte[size];
                byte* ptr = (byte*)address.ToPointer();
                for (int i = 0; i < size; i++)
                {
                    buffer[i] = ptr[i];
                }

                using (ArrowStreamReader reader = new ArrowStreamReader(new ReadOnlyMemory<byte>(buffer)))
                {
                    RecordBatch batch2 = reader.ReadNextRecordBatch();
                    Assert.Equal("None", batch2.Schema.FieldsList[0].Name);
                    Assert.Equal("", batch2.Schema.FieldsList[1].Name);
                    Assert.Equal("column", batch2.Schema.FieldsList[2].Name);
                    Assert.Equal("column", batch2.Schema.FieldsList[3].Name);
                }
            }
        }

        private static PyObject List(params int?[] values)
        {
            return new PyList(values.Select(i => i == null ? PyObject.None : new PyInt(i.Value)).ToArray());
        }

        private static PyObject List(params long?[] values)
        {
            return new PyList(values.Select(i => i == null ? PyObject.None : new PyInt(i.Value)).ToArray());
        }

        private static PyObject List(params double?[] values)
        {
            return new PyList(values.Select(i => i == null ? PyObject.None : new PyFloat(i.Value)).ToArray());
        }

        private static PyObject List(params string[] values)
        {
            return new PyList(values.Select(i => i == null ? PyObject.None : new PyString(i)).ToArray());
        }

        private static PyObject List(params PyObject[] values)
        {
            return new PyList(values);
        }

        private static PyObject Tuple(params int?[] values)
        {
            return new PyTuple(values.Select(i => i == null ? PyObject.None : new PyInt(i.Value)).ToArray());
        }

        sealed class TestArrayStream : IArrowArrayStream
        {
            private readonly RecordBatch[] _batches;
            private int _index;

            public TestArrayStream(Schema schema, params RecordBatch[] batches)
            {
                Schema = schema;
                _batches = batches;
            }

            public Schema Schema { get; }

            public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_index < 0) { throw new ObjectDisposedException(nameof(TestArrayStream)); }

                RecordBatch result = _index < _batches.Length ? _batches[_index++] : null;
                return new ValueTask<RecordBatch>(result);
            }

            public void Dispose()
            {
                _index = -1;
            }
        }
    }
}

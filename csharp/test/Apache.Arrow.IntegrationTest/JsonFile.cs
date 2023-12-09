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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Apache.Arrow.IntegrationTest
{
    public class JsonFile
    {
        public JsonSchema Schema { get; set; }

        public List<JsonDictionary> Dictionaries { get; set; }

        public List<JsonRecordBatch> Batches { get; set; }

        public static async ValueTask<JsonFile> ParseAsync(FileInfo fileInfo)
        {
            using var fileStream = fileInfo.OpenRead();
            var options = GetJsonOptions();
            return await JsonSerializer.DeserializeAsync<JsonFile>(fileStream, options);
        }

        public static JsonFile Parse(FileInfo fileInfo)
        {
            using var fileStream = fileInfo.OpenRead();
            var options = GetJsonOptions();
            return JsonSerializer.Deserialize<JsonFile>(fileStream, options);
        }

        public Schema GetSchemaAndDictionaries(out Func<DictionaryType, IArrowArray> dictionaries)
        {
            Schema schema = Schema.ToArrow(out Dictionary<DictionaryType, int> dictionaryIndexes);

            Func<DictionaryType, IArrowArray> lookup = null;
            lookup = type => Dictionaries.Single(d => d.Id == dictionaryIndexes[type]).Data.ToArrow(type.ValueType, lookup);
            dictionaries = lookup;

            return schema;
        }

        private static JsonSerializerOptions GetJsonOptions()
        {
            JsonSerializerOptions options = new JsonSerializerOptions()
            {
                PropertyNamingPolicy = JsonFileNamingPolicy.Instance,
            };
            options.Converters.Add(new ValidityConverter());
            return options;
        }
    }

    public class JsonSchema
    {
        public List<JsonField> Fields { get; set; }
        public JsonMetadata Metadata { get; set; }

        /// <summary>
        /// Decode this JSON schema as a Schema instance.
        /// </summary>
        public Schema ToArrow(out Dictionary<DictionaryType, int> dictionaryIndexes)
        {
            dictionaryIndexes = new Dictionary<DictionaryType, int>();
            return CreateSchema(this, dictionaryIndexes);
        }

        /// <summary>
        /// Decode this JSON schema as a Schema instance without computing dictionaries.
        /// This method is used by C Data Interface integration testing.
        /// </summary>
        public Schema ToArrow()
        {
            Dictionary<DictionaryType, int> dictionaryIndexes = new Dictionary<DictionaryType, int>();
            return CreateSchema(this, dictionaryIndexes);
        }

        private static Schema CreateSchema(JsonSchema jsonSchema, Dictionary<DictionaryType, int> dictionaryIndexes)
        {
            Schema.Builder builder = new Schema.Builder();
            for (int i = 0; i < jsonSchema.Fields.Count; i++)
            {
                builder.Field(f => CreateField(f, jsonSchema.Fields[i], dictionaryIndexes));
            }
            return builder.Build();
        }

        private static void CreateField(Field.Builder builder, JsonField jsonField, Dictionary<DictionaryType, int> dictionaryIndexes)
        {
            Field[] children = null;
            if (jsonField.Children?.Count > 0)
            {
                children = new Field[jsonField.Children.Count];
                for (int i = 0; i < jsonField.Children.Count; i++)
                {
                    Field.Builder field = new Field.Builder();
                    CreateField(field, jsonField.Children[i], dictionaryIndexes);
                    children[i] = field.Build();
                }
            }

            IArrowType type = ToArrowType(jsonField.Type, children);

            if (jsonField.Dictionary != null)
            {
                DictionaryType dictType = new DictionaryType(
                    ToArrowType(jsonField.Dictionary.IndexType, new Field[0]),
                    type,
                    jsonField.Dictionary.IsOrdered);

                dictionaryIndexes[dictType] = jsonField.Dictionary.Id;
                type = dictType;
            }

            builder.Name(jsonField.Name)
                .DataType(type)
                .Nullable(jsonField.Nullable);

            if (jsonField.Metadata != null)
            {
                builder.Metadata(jsonField.Metadata);
            }
        }

        private static IArrowType ToArrowType(JsonArrowType type, Field[] children)
        {
            return type.Name switch
            {
                "bool" => BooleanType.Default,
                "int" => ToIntArrowType(type),
                "floatingpoint" => ToFloatingPointArrowType(type),
                "decimal" => ToDecimalArrowType(type),
                "binary" => BinaryType.Default,
                "utf8" => StringType.Default,
                "fixedsizebinary" => new FixedSizeBinaryType(type.ByteWidth),
                "date" => ToDateArrowType(type),
                "time" => ToTimeArrowType(type),
                "duration" => ToDurationArrowType(type),
                "timestamp" => ToTimestampArrowType(type),
                "list" => ToListArrowType(type, children),
                "fixedsizelist" => ToFixedSizeListArrowType(type, children),
                "struct" => ToStructArrowType(type, children),
                "union" => ToUnionArrowType(type, children),
                "map" => ToMapArrowType(type, children),
                "null" => NullType.Default,
                _ => throw new NotSupportedException($"JsonArrowType not supported: {type.Name}")
            };
        }

        private static IArrowType ToIntArrowType(JsonArrowType type)
        {
            return (type.BitWidth, type.IsSigned) switch
            {
                (8, true) => Int8Type.Default,
                (8, false) => UInt8Type.Default,
                (16, true) => Int16Type.Default,
                (16, false) => UInt16Type.Default,
                (32, true) => Int32Type.Default,
                (32, false) => UInt32Type.Default,
                (64, true) => Int64Type.Default,
                (64, false) => UInt64Type.Default,
                _ => throw new NotSupportedException($"Int type not supported: {type.BitWidth}, {type.IsSigned}")
            };
        }

        private static IArrowType ToFloatingPointArrowType(JsonArrowType type)
        {
            return type.FloatingPointPrecision switch
            {
                "SINGLE" => FloatType.Default,
                "DOUBLE" => DoubleType.Default,
                _ => throw new NotSupportedException($"FloatingPoint type not supported: {type.FloatingPointPrecision}")
            };
        }

        private static IArrowType ToDecimalArrowType(JsonArrowType type)
        {
            return type.BitWidth switch
            {
                256 => new Decimal256Type(type.DecimalPrecision, type.Scale),
                _ => new Decimal128Type(type.DecimalPrecision, type.Scale),
            };
        }

        private static IArrowType ToDateArrowType(JsonArrowType type)
        {
            return type.Unit switch
            {
                "DAY" => Date32Type.Default,
                "MILLISECOND" => Date64Type.Default,
                _ => throw new NotSupportedException($"Date type not supported: {type.Unit}")
            };
        }

        private static IArrowType ToTimeArrowType(JsonArrowType type)
        {
            return (type.Unit, type.BitWidth) switch
            {
                ("SECOND", 32) => new Time32Type(TimeUnit.Second),
                ("SECOND", 64) => new Time64Type(TimeUnit.Second),
                ("MILLISECOND", 32) => new Time32Type(TimeUnit.Millisecond),
                ("MILLISECOND", 64) => new Time64Type(TimeUnit.Millisecond),
                ("MICROSECOND", 32) => new Time32Type(TimeUnit.Microsecond),
                ("MICROSECOND", 64) => new Time64Type(TimeUnit.Microsecond),
                ("NANOSECOND", 32) => new Time32Type(TimeUnit.Nanosecond),
                ("NANOSECOND", 64) => new Time64Type(TimeUnit.Nanosecond),
                _ => throw new NotSupportedException($"Time type not supported: {type.Unit}, {type.BitWidth}")
            };
        }

        private static IArrowType ToDurationArrowType(JsonArrowType type)
        {
            return type.Unit switch
            {
                "SECOND" => DurationType.Second,
                "MILLISECOND" => DurationType.Millisecond,
                "MICROSECOND" => DurationType.Microsecond,
                "NANOSECOND" => DurationType.Nanosecond,
                _ => throw new NotSupportedException($"Time type not supported: {type.Unit}, {type.BitWidth}")
            };
        }

        private static IArrowType ToTimestampArrowType(JsonArrowType type)
        {
            return type.Unit switch
            {
                "SECOND" => new TimestampType(TimeUnit.Second, type.Timezone),
                "MILLISECOND" => new TimestampType(TimeUnit.Millisecond, type.Timezone),
                "MICROSECOND" => new TimestampType(TimeUnit.Microsecond, type.Timezone),
                "NANOSECOND" => new TimestampType(TimeUnit.Nanosecond, type.Timezone),
                _ => throw new NotSupportedException($"Time type not supported: {type.Unit}, {type.BitWidth}")
            };
        }

        private static IArrowType ToListArrowType(JsonArrowType type, Field[] children)
        {
            return new ListType(children[0]);
        }

        private static IArrowType ToFixedSizeListArrowType(JsonArrowType type, Field[] children)
        {
            return new FixedSizeListType(children[0], type.ListSize);
        }

        private static IArrowType ToStructArrowType(JsonArrowType type, Field[] children)
        {
            return new StructType(children);
        }

        private static IArrowType ToUnionArrowType(JsonArrowType type, Field[] children)
        {
            UnionMode mode = type.Mode switch
            {
                "SPARSE" => UnionMode.Sparse,
                "DENSE" => UnionMode.Dense,
                _ => throw new NotSupportedException($"Union mode not supported: {type.Mode}"),
            };
            return new UnionType(children, type.TypeIds, mode);
        }

        private static IArrowType ToMapArrowType(JsonArrowType type, Field[] children)
        {
            return new MapType(children[0], type.KeysSorted);
        }
    }

    public class JsonField
    {
        public string Name { get; set; }
        public bool Nullable { get; set; }
        public JsonArrowType Type { get; set; }
        public List<JsonField> Children { get; set; }
        public JsonDictionaryIndex Dictionary { get; set; }
        public JsonMetadata Metadata { get; set; }
    }

    public class JsonArrowType
    {
        public string Name { get; set; }

        // int fields
        public int BitWidth { get; set; }
        public bool IsSigned { get; set; }

        // floating point fields
        [JsonIgnore]
        public string FloatingPointPrecision => ExtensionData["precision"].GetString();

        // decimal fields
        [JsonIgnore]
        public int DecimalPrecision => ExtensionData["precision"].GetInt32();
        public int Scale { get; set; }

        // date and time fields
        public string Unit { get; set; }
        // timestamp fields
        public string Timezone { get; set; }

        // FixedSizeBinary fields
        public int ByteWidth { get; set; }

        // FixedSizeList fields
        public int ListSize { get; set; }

        // union fields
        public string Mode { get; set; }
        public int[] TypeIds { get; set; }

        // map fields
        public bool KeysSorted { get; set; }

        [JsonExtensionData]
        public Dictionary<string, JsonElement> ExtensionData { get; set; }
    }

    public class JsonDictionaryIndex
    {
        public int Id { get; set; }
        public JsonArrowType IndexType { get; set; }
        public bool IsOrdered { get; set; }
    }

    public class JsonDictionary
    {
        public int Id { get; set; }

        [JsonPropertyName("data")]
        public JsonRecordBatch Data { get; set; }
    }

    public class JsonMetadata : List<KeyValuePair<string, string>>
    {
    }

    public class JsonRecordBatch
    {
        public int Count { get; set; }
        public List<JsonFieldData> Columns { get; set; }

        /// <summary>
        /// Decode this JSON record batch as a RecordBatch instance.
        /// </summary>
        public RecordBatch ToArrow(Schema schema, Func<DictionaryType, IArrowArray> dictionaries)
        {
            return CreateRecordBatch(schema, dictionaries, this);
        }

        /// <summary>
        /// Decode this JSON record batch as a RecordBatch instance without supporting dictionaries.
        /// This method is used by C Data Interface integration testing.
        /// </summary>
        public RecordBatch ToArrow(Schema schema)
        {
            return CreateRecordBatch(schema, _ => throw new NotImplementedException(), this);
        }

        public IArrowArray ToArrow(IArrowType arrowType, Func<DictionaryType, IArrowArray> dictionaries)
        {
            ArrayCreator creator = new ArrayCreator(this.Columns[0], dictionaries);
            arrowType.Accept(creator);
            return creator.Array;
        }

        private RecordBatch CreateRecordBatch(Schema schema, Func<DictionaryType, IArrowArray> dictionaries, JsonRecordBatch jsonRecordBatch)
        {
            if (schema.FieldsList.Count != jsonRecordBatch.Columns.Count)
            {
                throw new NotSupportedException($"jsonRecordBatch.Columns.Count '{jsonRecordBatch.Columns.Count}' doesn't match schema field count '{schema.FieldsList.Count}'");
            }

            List<IArrowArray> arrays = new List<IArrowArray>(jsonRecordBatch.Columns.Count);
            for (int i = 0; i < jsonRecordBatch.Columns.Count; i++)
            {
                JsonFieldData data = jsonRecordBatch.Columns[i];
                Field field = schema.FieldsList[i];
                ArrayCreator creator = new ArrayCreator(data, dictionaries);
                field.DataType.Accept(creator);
                arrays.Add(creator.Array);
            }

            return new RecordBatch(schema, arrays, jsonRecordBatch.Count);
        }

        private class ArrayCreator :
            IArrowTypeVisitor<BooleanType>,
            IArrowTypeVisitor<Int8Type>,
            IArrowTypeVisitor<Int16Type>,
            IArrowTypeVisitor<Int32Type>,
            IArrowTypeVisitor<Int64Type>,
            IArrowTypeVisitor<UInt8Type>,
            IArrowTypeVisitor<UInt16Type>,
            IArrowTypeVisitor<UInt32Type>,
            IArrowTypeVisitor<UInt64Type>,
            IArrowTypeVisitor<FloatType>,
            IArrowTypeVisitor<DoubleType>,
            IArrowTypeVisitor<Decimal128Type>,
            IArrowTypeVisitor<Decimal256Type>,
            IArrowTypeVisitor<Date32Type>,
            IArrowTypeVisitor<Date64Type>,
            IArrowTypeVisitor<Time32Type>,
            IArrowTypeVisitor<Time64Type>,
            IArrowTypeVisitor<DurationType>,
            IArrowTypeVisitor<TimestampType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<BinaryType>,
            IArrowTypeVisitor<FixedSizeBinaryType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<FixedSizeListType>,
            IArrowTypeVisitor<StructType>,
            IArrowTypeVisitor<UnionType>,
            IArrowTypeVisitor<MapType>,
            IArrowTypeVisitor<DictionaryType>,
            IArrowTypeVisitor<NullType>
        {
            private JsonFieldData JsonFieldData { get; set; }
            public IArrowArray Array { get; private set; }

            private readonly Func<DictionaryType, IArrowArray> dictionaries;

            public ArrayCreator(JsonFieldData jsonFieldData, Func<DictionaryType, IArrowArray> dictionaries)
            {
                JsonFieldData = jsonFieldData;
                this.dictionaries = dictionaries;
            }

            public void Visit(BooleanType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);
                ArrowBuffer.BitmapBuilder valueBuilder = new ArrowBuffer.BitmapBuilder(validityBuffer.Length);

                var json = JsonFieldData.Data.GetRawText();
                bool[] values = JsonSerializer.Deserialize<bool[]>(json);

                foreach (bool value in values)
                {
                    valueBuilder.Append(value);
                }
                ArrowBuffer valueBuffer = valueBuilder.Build();

                Array = new BooleanArray(
                    valueBuffer, validityBuffer,
                    JsonFieldData.Count, nullCount, 0);
            }

            public void Visit(Int8Type type) => GenerateArray<sbyte, Int8Array>((v, n, c, nc, o) => new Int8Array(v, n, c, nc, o));
            public void Visit(Int16Type type) => GenerateArray<short, Int16Array>((v, n, c, nc, o) => new Int16Array(v, n, c, nc, o));
            public void Visit(Int32Type type) => GenerateArray<int, Int32Array>((v, n, c, nc, o) => new Int32Array(v, n, c, nc, o));
            public void Visit(Int64Type type) => GenerateLongArray<long, Int64Array>((v, n, c, nc, o) => new Int64Array(v, n, c, nc, o), s => long.Parse(s));
            public void Visit(UInt8Type type) => GenerateArray<byte, UInt8Array>((v, n, c, nc, o) => new UInt8Array(v, n, c, nc, o));
            public void Visit(UInt16Type type) => GenerateArray<ushort, UInt16Array>((v, n, c, nc, o) => new UInt16Array(v, n, c, nc, o));
            public void Visit(UInt32Type type) => GenerateArray<uint, UInt32Array>((v, n, c, nc, o) => new UInt32Array(v, n, c, nc, o));
            public void Visit(UInt64Type type) => GenerateLongArray<ulong, UInt64Array>((v, n, c, nc, o) => new UInt64Array(v, n, c, nc, o), s => ulong.Parse(s));
            public void Visit(FloatType type) => GenerateArray<float, FloatArray>((v, n, c, nc, o) => new FloatArray(v, n, c, nc, o));
            public void Visit(DoubleType type) => GenerateArray<double, DoubleArray>((v, n, c, nc, o) => new DoubleArray(v, n, c, nc, o));
            public void Visit(Time32Type type) => GenerateArray<int, Time32Array>((v, n, c, nc, o) => new Time32Array(type, v, n, c, nc, o));
            public void Visit(Time64Type type) => GenerateLongArray<long, Time64Array>((v, n, c, nc, o) => new Time64Array(type, v, n, c, nc, o), s => long.Parse(s));
            public void Visit(DurationType type) => GenerateLongArray<long, DurationArray>((v, n, c, nc, o) => new DurationArray(type, v, n, c, nc, o), s => long.Parse(s));

            public void Visit(Decimal128Type type)
            {
                Array = new Decimal128Array(GetDecimalArrayData(type));
            }

            public void Visit(Decimal256Type type)
            {
                Array = new Decimal256Array(GetDecimalArrayData(type));
            }

            public void Visit(NullType type)
            {
                Array = new NullArray(JsonFieldData.Count);
            }

            private ArrayData GetDecimalArrayData(FixedSizeBinaryType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);

                var json = JsonFieldData.Data.GetRawText();
                string[] values = JsonSerializer.Deserialize<string[]>(json, s_options);

                Span<byte> buffer = stackalloc byte[type.ByteWidth];

                ArrowBuffer.Builder<byte> valueBuilder = new ArrowBuffer.Builder<byte>();
                foreach (string value in values)
                {
                    buffer.Fill(0);

                    BigInteger bigInteger = BigInteger.Parse(value);
                    if (!bigInteger.TryWriteBytes(buffer, out int bytesWritten, false, !BitConverter.IsLittleEndian))
                    {
                        throw new InvalidDataException($"Decimal data was too big to fit into {type.BitWidth} bits.");
                    }

                    if (bigInteger.Sign == -1)
                    {
                        buffer.Slice(bytesWritten).Fill(255);
                    }

                    valueBuilder.Append(buffer);
                }
                ArrowBuffer valueBuffer = valueBuilder.Build(default);

                return new ArrayData(type, JsonFieldData.Count, nullCount, 0, new[] { validityBuffer, valueBuffer });
            }

            public void Visit(Date32Type type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);

                ArrowBuffer.Builder<int> valueBuilder = new ArrowBuffer.Builder<int>(JsonFieldData.Count);
                var json = JsonFieldData.Data.GetRawText();
                int[] values = JsonSerializer.Deserialize<int[]>(json, s_options);

                foreach (int value in values)
                {
                    valueBuilder.Append(value);
                }
                ArrowBuffer valueBuffer = valueBuilder.Build();

                Array = new Date32Array(
                    valueBuffer, validityBuffer,
                    JsonFieldData.Count, nullCount, 0);
            }

            public void Visit(Date64Type type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);

                ArrowBuffer.Builder<long> valueBuilder = new ArrowBuffer.Builder<long>(JsonFieldData.Count);
                var json = JsonFieldData.Data.GetRawText();
                string[] values = JsonSerializer.Deserialize<string[]>(json, s_options);

                foreach (string value in values)
                {
                    valueBuilder.Append(long.Parse(value));
                }
                ArrowBuffer valueBuffer = valueBuilder.Build();

                Array = new Date64Array(
                    valueBuffer, validityBuffer,
                    JsonFieldData.Count, nullCount, 0);
            }

            public void Visit(TimestampType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);

                ArrowBuffer.Builder<long> valueBuilder = new ArrowBuffer.Builder<long>(JsonFieldData.Count);
                var json = JsonFieldData.Data.GetRawText();
                string[] values = JsonSerializer.Deserialize<string[]>(json, s_options);

                foreach (string value in values)
                {
                    valueBuilder.Append(long.Parse(value));
                }
                ArrowBuffer valueBuffer = valueBuilder.Build();

                Array = new TimestampArray(
                    type, valueBuffer, validityBuffer,
                    JsonFieldData.Count, nullCount, 0);
            }

            public void Visit(StringType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);
                ArrowBuffer offsetBuffer = GetOffsetBuffer();

                var json = JsonFieldData.Data.GetRawText();
                string[] values = JsonSerializer.Deserialize<string[]>(json, s_options);

                ArrowBuffer.Builder<byte> valueBuilder = new ArrowBuffer.Builder<byte>();
                foreach (string value in values)
                {
                    valueBuilder.Append(Encoding.UTF8.GetBytes(value));
                }
                ArrowBuffer valueBuffer = valueBuilder.Build(default);

                Array = new StringArray(JsonFieldData.Count, offsetBuffer, valueBuffer, validityBuffer, nullCount);
            }

            public void Visit(BinaryType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);
                ArrowBuffer offsetBuffer = GetOffsetBuffer();

                var json = JsonFieldData.Data.GetRawText();
                string[] values = JsonSerializer.Deserialize<string[]>(json, s_options);

                ArrowBuffer.Builder<byte> valueBuilder = new ArrowBuffer.Builder<byte>();
                foreach (string value in values)
                {
                    valueBuilder.Append(ConvertHexStringToByteArray(value));
                }
                ArrowBuffer valueBuffer = valueBuilder.Build(default);

                ArrayData arrayData = new ArrayData(type, JsonFieldData.Count, nullCount, 0, new[] { validityBuffer, offsetBuffer, valueBuffer });
                Array = new BinaryArray(arrayData);
            }

            public void Visit(FixedSizeBinaryType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);

                var json = JsonFieldData.Data.GetRawText();
                string[] values = JsonSerializer.Deserialize<string[]>(json, s_options);

                ArrowBuffer.Builder<byte> valueBuilder = new ArrowBuffer.Builder<byte>();
                foreach (string value in values)
                {
                    valueBuilder.Append(ConvertHexStringToByteArray(value));
                }
                ArrowBuffer valueBuffer = valueBuilder.Build(default);

                ArrayData arrayData = new ArrayData(type, JsonFieldData.Count, nullCount, 0, new[] { validityBuffer, valueBuffer });
                Array = new FixedSizeBinaryArray(arrayData);
            }

            public void Visit(ListType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);
                ArrowBuffer offsetBuffer = GetOffsetBuffer();

                var data = JsonFieldData;
                JsonFieldData = data.Children[0];
                type.ValueDataType.Accept(this);
                JsonFieldData = data;

                ArrayData arrayData = new ArrayData(type, JsonFieldData.Count, nullCount, 0,
                    new[] { validityBuffer, offsetBuffer }, new[] { Array.Data });
                Array = new ListArray(arrayData);
            }

            public void Visit(FixedSizeListType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);

                var data = JsonFieldData;
                JsonFieldData = data.Children[0];
                type.ValueDataType.Accept(this);
                JsonFieldData = data;

                ArrayData arrayData = new ArrayData(type, JsonFieldData.Count, nullCount, 0,
                    new[] { validityBuffer }, new[] { Array.Data });
                Array = new FixedSizeListArray(arrayData);
            }

            public void Visit(StructType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);

                ArrayData[] children = new ArrayData[type.Fields.Count];

                var data = JsonFieldData;
                for (int i = 0; i < children.Length; i++)
                {
                    JsonFieldData = data.Children[i];
                    type.Fields[i].DataType.Accept(this);
                    children[i] = Array.Data;
                }
                JsonFieldData = data;

                ArrayData arrayData = new ArrayData(type, JsonFieldData.Count, nullCount, 0,
                    new[] { validityBuffer }, children);
                Array = new StructArray(arrayData);
            }

            public void Visit(UnionType type)
            {
                ArrowBuffer[] buffers;
                if (type.Mode == UnionMode.Dense)
                {
                    buffers = new ArrowBuffer[2];
                    buffers[1] = GetOffsetBuffer();
                }
                else
                {
                    buffers = new ArrowBuffer[1];
                }
                buffers[0] = GetTypeIdBuffer();

                ArrayData[] children = GetChildren(type);

                int nullCount = 0;
                ArrayData arrayData = new ArrayData(type, JsonFieldData.Count, nullCount, 0, buffers, children);
                Array = UnionArray.Create(arrayData);
            }

            public void Visit(MapType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);
                ArrowBuffer offsetBuffer = GetOffsetBuffer();

                var data = JsonFieldData;
                JsonFieldData = data.Children[0];
                type.KeyValueType.Accept(this);
                JsonFieldData = data;

                ArrayData arrayData = new ArrayData(type, JsonFieldData.Count, nullCount, 0,
                    new[] { validityBuffer, offsetBuffer }, new[] { Array.Data });
                Array = new MapArray(arrayData);
            }

            public void Visit(DictionaryType type)
            {
                type.IndexType.Accept(this);
                Array = new DictionaryArray(type, Array, this.dictionaries(type));
            }

            private ArrayData[] GetChildren(NestedType type)
            {
                ArrayData[] children = new ArrayData[type.Fields.Count];

                var data = JsonFieldData;
                for (int i = 0; i < children.Length; i++)
                {
                    JsonFieldData = data.Children[i];
                    type.Fields[i].DataType.Accept(this);
                    children[i] = Array.Data;
                }
                JsonFieldData = data;

                return children;
            }

            private static byte[] ConvertHexStringToByteArray(string hexString)
            {
                byte[] data = new byte[hexString.Length / 2];
                for (int index = 0; index < data.Length; index++)
                {
                    data[index] = byte.Parse(hexString.AsSpan(index * 2, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
                }

                return data;
            }

            private static readonly JsonSerializerOptions s_options = new JsonSerializerOptions()
            {
                Converters =
                {
                    new ByteArrayConverter()
                }
            };

            private void GenerateArray<T, TArray>(Func<ArrowBuffer, ArrowBuffer, int, int, int, TArray> createArray)
                where TArray : PrimitiveArray<T>
                where T : struct
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);

                ArrowBuffer.Builder<T> valueBuilder = new ArrowBuffer.Builder<T>(JsonFieldData.Count);
                var json = JsonFieldData.Data.GetRawText();
                T[] values = JsonSerializer.Deserialize<T[]>(json, s_options);

                foreach (T value in values)
                {
                    valueBuilder.Append(value);
                }
                ArrowBuffer valueBuffer = valueBuilder.Build();

                Array = createArray(
                    valueBuffer, validityBuffer,
                    JsonFieldData.Count, nullCount, 0);
            }

            private void GenerateLongArray<T, TArray>(Func<ArrowBuffer, ArrowBuffer, int, int, int, TArray> createArray, Func<string, T> parse)
                where TArray : PrimitiveArray<T>
                where T : struct
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);

                ArrowBuffer.Builder<T> valueBuilder = new ArrowBuffer.Builder<T>(JsonFieldData.Count);
                var json = JsonFieldData.Data.GetRawText();
                string[] values = JsonSerializer.Deserialize<string[]>(json);

                foreach (string value in values)
                {
                    valueBuilder.Append(parse(value));
                }
                ArrowBuffer valueBuffer = valueBuilder.Build();

                Array = createArray(
                    valueBuffer, validityBuffer,
                    JsonFieldData.Count, nullCount, 0);
            }

            private ArrowBuffer GetOffsetBuffer()
            {
                ArrowBuffer.Builder<int> valueOffsets = new ArrowBuffer.Builder<int>(JsonFieldData.Offset.Length);
                valueOffsets.AppendRange(JsonFieldData.Offset);
                return valueOffsets.Build(default);
            }

            private ArrowBuffer GetTypeIdBuffer()
            {
                ArrowBuffer.Builder<byte> typeIds = new ArrowBuffer.Builder<byte>(JsonFieldData.TypeId.Length);
                for (int i = 0; i < JsonFieldData.TypeId.Length; i++)
                {
                    typeIds.Append(checked((byte)JsonFieldData.TypeId[i]));
                }
                return typeIds.Build(default);
            }

            private ArrowBuffer GetValidityBuffer(out int nullCount)
            {
                if (JsonFieldData.Validity == null)
                {
                    nullCount = 0;
                    return ArrowBuffer.Empty;
                }

                ArrowBuffer.BitmapBuilder validityBuilder = new ArrowBuffer.BitmapBuilder(JsonFieldData.Validity.Length);
                validityBuilder.AppendRange(JsonFieldData.Validity);

                nullCount = validityBuilder.UnsetBitCount;
                return validityBuilder.Build();
            }

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException($"{type.Name} not implemented");
            }
        }
    }

    public class JsonFieldData
    {
        public string Name { get; set; }
        public int Count { get; set; }
        public bool[] Validity { get; set; }
        public int[] Offset { get; set; }
        public int[] TypeId { get; set; }
        public JsonElement Data { get; set; }
        public List<JsonFieldData> Children { get; set; }
    }

    internal sealed class ValidityConverter : JsonConverter<bool>
    {
        public override bool Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.True) return true;
            if (reader.TokenType == JsonTokenType.False) return false;

            if (typeToConvert != typeof(bool) || reader.TokenType != JsonTokenType.Number)
            {
                throw new InvalidOperationException($"Unexpected bool data: {reader.TokenType}");
            }

            int value = reader.GetInt32();
            if (value == 0) return false;
            if (value == 1) return true;

            throw new InvalidOperationException($"Unexpected bool value: {value}");
        }

        public override void Write(Utf8JsonWriter writer, bool value, JsonSerializerOptions options) => throw new NotImplementedException();
    }

    internal sealed class ByteArrayConverter : JsonConverter<byte[]>
    {
        public override byte[] Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartArray)
            {
                throw new InvalidOperationException($"Unexpected byte[] token: {reader.TokenType}");
            }

            List<byte> values = new List<byte>();
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndArray)
                {
                    return values.ToArray();
                }

                if (reader.TokenType != JsonTokenType.Number)
                {
                    throw new InvalidOperationException($"Unexpected byte token: {reader.TokenType}");
                }

                values.Add(reader.GetByte());
            }

            throw new InvalidOperationException("Unexpectedly reached the end of the reader");
        }

        public override void Write(Utf8JsonWriter writer, byte[] value, JsonSerializerOptions options) => throw new NotImplementedException();
    }

    internal sealed class JsonFileNamingPolicy : JsonNamingPolicy
    {
        public static JsonFileNamingPolicy Instance { get; } = new JsonFileNamingPolicy();

        public override string ConvertName(string name)
        {
            if (name == "Validity")
            {
                return "VALIDITY";
            }
            else if (name == "Offset")
            {
                return "OFFSET";
            }
            else if (name == "TypeId")
            {
                return "TYPE_ID";
            }
            else if (name == "Data")
            {
                return "DATA";
            }
            else
            {
                return CamelCase.ConvertName(name);
            }
        }
    }
}

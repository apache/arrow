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
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Apache.Arrow.Arrays;
using Apache.Arrow.Ipc;
using Apache.Arrow.Tests;
using Apache.Arrow.Types;

namespace Apache.Arrow.IntegrationTest
{
    public class IntegrationCommand
    {
        public string Mode { get; set; }
        public FileInfo JsonFileInfo { get; set; }
        public FileInfo ArrowFileInfo { get; set; }

        public IntegrationCommand(string mode, FileInfo jsonFileInfo, FileInfo arrowFileInfo)
        {
            Mode = mode;
            JsonFileInfo = jsonFileInfo;
            ArrowFileInfo = arrowFileInfo;
        }

        public async Task<int> Execute()
        {
            Func<Task<int>> commandDelegate = Mode switch
            {
                "validate" => Validate,
                "json-to-arrow" => JsonToArrow,
                "stream-to-file" => StreamToFile,
                "file-to-stream" => FileToStream,
                _ => () =>
                {
                    Console.WriteLine($"Mode '{Mode}' is not supported.");
                    return Task.FromResult(-1);
                }
            };
            return await commandDelegate();
        }

        private async Task<int> Validate()
        {
            JsonFile jsonFile = await ParseJsonFile();

            using FileStream arrowFileStream = ArrowFileInfo.OpenRead();
            using ArrowFileReader reader = new ArrowFileReader(arrowFileStream);
            int batchCount = await reader.RecordBatchCountAsync();

            if (batchCount != jsonFile.Batches.Count)
            {
                Console.WriteLine($"Incorrect batch count. JsonFile: {jsonFile.Batches.Count}, ArrowFile: {batchCount}");
                return -1;
            }

            Schema jsonFileSchema = CreateSchema(jsonFile.Schema);
            Schema arrowFileSchema = reader.Schema;

            SchemaComparer.Compare(jsonFileSchema, arrowFileSchema);

            for (int i = 0; i < batchCount; i++)
            {
                RecordBatch arrowFileRecordBatch = reader.ReadNextRecordBatch();
                RecordBatch jsonFileRecordBatch = CreateRecordBatch(jsonFileSchema, jsonFile.Batches[i]);

                ArrowReaderVerifier.CompareBatches(jsonFileRecordBatch, arrowFileRecordBatch, strictCompare: false);
            }

            // ensure there are no more batches in the file
            if (reader.ReadNextRecordBatch() != null)
            {
                Console.WriteLine($"The ArrowFile has more RecordBatches than it should.");
                return -1;
            }

            return 0;
        }

        private async Task<int> JsonToArrow()
        {
            JsonFile jsonFile = await ParseJsonFile();
            Schema schema = CreateSchema(jsonFile.Schema);

            using (FileStream fs = ArrowFileInfo.Create())
            {
                ArrowFileWriter writer = new ArrowFileWriter(fs, schema);
                await writer.WriteStartAsync();

                foreach (var jsonRecordBatch in jsonFile.Batches)
                {
                    RecordBatch batch = CreateRecordBatch(schema, jsonRecordBatch);
                    await writer.WriteRecordBatchAsync(batch);
                }
                await writer.WriteEndAsync();
                await fs.FlushAsync();
            }

            return 0;
        }

        private RecordBatch CreateRecordBatch(Schema schema, JsonRecordBatch jsonRecordBatch)
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
                ArrayCreator creator = new ArrayCreator(data);
                field.DataType.Accept(creator);
                arrays.Add(creator.Array);
            }

            return new RecordBatch(schema, arrays, jsonRecordBatch.Count);
        }

        private static Schema CreateSchema(JsonSchema jsonSchema)
        {
            Schema.Builder builder = new Schema.Builder();
            for (int i = 0; i < jsonSchema.Fields.Count; i++)
            {
                builder.Field(f => CreateField(f, jsonSchema.Fields[i]));
            }
            return builder.Build();
        }

        private static void CreateField(Field.Builder builder, JsonField jsonField)
        {
            Field[] children = null;
            if (jsonField.Children?.Count > 0)
            {
                children = new Field[jsonField.Children.Count];
                for (int i = 0; i < jsonField.Children.Count; i++)
                {
                    Field.Builder field = new Field.Builder();
                    CreateField(field, jsonField.Children[i]);
                    children[i] = field.Build();
                }
            }

            builder.Name(jsonField.Name)
                .DataType(ToArrowType(jsonField.Type, children))
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
                "timestamp" => ToTimestampArrowType(type),
                "list" => ToListArrowType(type, children),
                "fixedsizelist" => ToFixedSizeListArrowType(type, children),
                "struct" => ToStructArrowType(type, children),
                "union" => ToUnionArrowType(type, children),
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
            IArrowTypeVisitor<TimestampType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<BinaryType>,
            IArrowTypeVisitor<FixedSizeBinaryType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<FixedSizeListType>,
            IArrowTypeVisitor<StructType>,
            IArrowTypeVisitor<UnionType>,
            IArrowTypeVisitor<NullType>
        {
            private JsonFieldData JsonFieldData { get; set; }
            public IArrowArray Array { get; private set; }

            public ArrayCreator(JsonFieldData jsonFieldData)
            {
                JsonFieldData = jsonFieldData;
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
                if (JsonFieldData.Count == 0) { return ArrowBuffer.Empty; }
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

        private async Task<int> StreamToFile()
        {
            using ArrowStreamReader reader = new ArrowStreamReader(Console.OpenStandardInput());

            RecordBatch batch = await reader.ReadNextRecordBatchAsync();

            using FileStream fileStream = ArrowFileInfo.OpenWrite();
            using ArrowFileWriter writer = new ArrowFileWriter(fileStream, reader.Schema);
            await writer.WriteStartAsync();

            while (batch != null)
            {
                await writer.WriteRecordBatchAsync(batch);

                batch = await reader.ReadNextRecordBatchAsync();
            }

            await writer.WriteEndAsync();

            return 0;
        }

        private async Task<int> FileToStream()
        {
            using FileStream fileStream = ArrowFileInfo.OpenRead();
            using ArrowFileReader fileReader = new ArrowFileReader(fileStream);

            // read the record batch count to initialize the Schema
            await fileReader.RecordBatchCountAsync();

            using ArrowStreamWriter writer = new ArrowStreamWriter(Console.OpenStandardOutput(), fileReader.Schema);
            await writer.WriteStartAsync();

            RecordBatch batch;
            while ((batch = fileReader.ReadNextRecordBatch()) != null)
            {
                await writer.WriteRecordBatchAsync(batch);
            }

            await writer.WriteEndAsync();

            return 0;
        }

        private async ValueTask<JsonFile> ParseJsonFile()
        {
            using var fileStream = JsonFileInfo.OpenRead();
            JsonSerializerOptions options = new JsonSerializerOptions()
            {
                PropertyNamingPolicy = JsonFileNamingPolicy.Instance,
            };
            options.Converters.Add(new ValidityConverter());

            return await JsonSerializer.DeserializeAsync<JsonFile>(fileStream, options);
        }
    }
}

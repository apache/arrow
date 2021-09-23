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
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Apache.Arrow.IntegrationTest
{
    public class JsonFile
    {
        public JsonSchema Schema { get; set; }
        public List<JsonRecordBatch> Batches { get; set; }
        //public List<DictionaryBatch> Dictionaries {get;set;}
    }

    public class JsonSchema
    {
        public List<JsonField> Fields { get; set; }
        public JsonMetadata Metadata { get; set; }
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

        [JsonExtensionData]
        public Dictionary<string, JsonElement> ExtensionData { get; set; }
    }

    public class JsonDictionaryIndex
    {
        public int Id { get; set; }
        public JsonArrowType Type { get; set; }
        public bool IsOrdered { get; set; }
    }

    public class JsonMetadata : List<KeyValuePair<string, string>>
    {
    }

    public class JsonRecordBatch
    {
        public int Count { get; set; }
        public List<JsonFieldData> Columns { get; set; }
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

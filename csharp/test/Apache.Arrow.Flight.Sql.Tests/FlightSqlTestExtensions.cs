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
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Type = System.Type;

namespace Apache.Arrow.Flight.Sql.Tests;

public static class FlightSqlTestExtensions
{
    public static ByteString PackAndSerialize(this IMessage command)
    {
        return Any.Pack(command).Serialize();
    }
}

internal static class TestSchemaExtensions
{
    public static void PrintSchema(this RecordBatch recordBatchResult)
    {
        // Display column headers
        foreach (var field in recordBatchResult.Schema.FieldsList)
        {
            Console.Write($"{field.Name}\t");
        }

        Console.WriteLine();

        int rowCount = recordBatchResult.Length;

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            foreach (var array in recordBatchResult.Arrays)
            {
                // Retrieve value based on array type
                if (array is Int32Array intArray)
                {
                    Console.Write($"{intArray.GetValue(rowIndex)}\t");
                }
                else if (array is StringArray stringArray)
                {
                    Console.Write($"{stringArray.GetString(rowIndex)}\t");
                }
                else if (array is Int64Array longArray)
                {
                    Console.Write($"{longArray.GetValue(rowIndex)}\t");
                }
                else if (array is FloatArray floatArray)
                {
                    Console.Write($"{floatArray.GetValue(rowIndex)}\t");
                }
                else if (array is BooleanArray boolArray)
                {
                    Console.Write($"{boolArray.GetValue(rowIndex)}\t");
                }
                else
                {
                    Console.Write("N/A\t"); // Fallback for unsupported types
                }
            }

            Console.WriteLine(); // Move to the next row
        }
    }

    public static RecordBatch CreateRecordBatch(int[] values)
    {
        var paramsList = new List<IArrowArray>();
        var schema = new Schema.Builder();
        for (var index = 0; index < values.Length; index++)
        {
            var val = values[index];
            var builder = new Int32Array.Builder();
            builder.Append(val);
            var paramsArray = builder.Build();
            paramsList.Add(paramsArray);
            schema.Field(f => f.Name($"param{index}").DataType(Int32Type.Default).Nullable(false));
        }

        return new RecordBatch(schema.Build(), paramsList, values.Length);
    }
    
    public static void PrintSchema(this Schema schema)
    {
        Console.WriteLine("Schema Fields:");
        Console.WriteLine("{0,-20} {1,-20} {2,-20}", "Field Name", "Field Type", "Is Nullable");
        Console.WriteLine(new string('-', 60));
    
        foreach (var field in schema.FieldsLookup)
        {
            string fieldName = field.First().Name;
            string fieldType = field.First().DataType.TypeId.ToString();
            string isNullable = field.First().IsNullable ? "Yes" : "No";

            Console.WriteLine("{0,-20} {1,-20} {2,-20}", fieldName, fieldType, isNullable);
        }
    }

    public static string GetStringValue(IArrowArray array, int index)
    {
        return array switch
        {
            StringArray stringArray => stringArray.GetString(index),
            Int32Array intArray => intArray.GetValue(index).ToString(),
            Int64Array longArray => longArray.GetValue(index).ToString(),
            BooleanArray boolArray => boolArray.GetValue(index).Value ? "true" : "false",
            _ => "Unsupported Type"
        };
    }
    
    public static void PrintRecordBatch(RecordBatch recordBatch)
    {
        int rowCount = recordBatch.Length;
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            string catalogName = GetStringValue(recordBatch.Column(0), rowIndex);
            string schemaName = GetStringValue(recordBatch.Column(1), rowIndex);
            string tableName = GetStringValue(recordBatch.Column(2), rowIndex);
            string tableType = GetStringValue(recordBatch.Column(3), rowIndex);

            Console.WriteLine("{0,-20} {1,-20} {2,-20} {3,-20}", catalogName, schemaName, tableName, tableType);
        }
    }

    public static RecordBatch CreateRecordBatch(int[] ids, string[] values)
    {
        var idArrayBuilder = new Int32Array.Builder();
        var valueArrayBuilder = new StringArray.Builder();

        for (int i = 0; i < ids.Length; i++)
        {
            idArrayBuilder.Append(ids[i]);
            valueArrayBuilder.Append(values[i]);
        }

        var schema = new Schema.Builder()
            .Field(f => f.Name("Id").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("Value").DataType(StringType.Default).Nullable(false))
            .Build();

        return new RecordBatch(schema, [idArrayBuilder.Build(), valueArrayBuilder.Build()], ids.Length);
    }
    
    public static RecordBatch CreateRecordBatch<T>(T[] items)
    {
        if (items is null || items.Length == 0)
        {
            throw new ArgumentException("Items array cannot be null or empty.");
        }

        var schema = BuildSchema(typeof(T));
        
        var arrays = new List<IArrowArray>();
        foreach (var field in schema.FieldsList)
        {
            var property = typeof(T).GetProperty(field.Name);
            if (property is null)
            {
                throw new InvalidOperationException($"Property {field.Name} not found in type {typeof(T).Name}.");
            }
            
            // extract values and build the array
            var values = items.Select(item => property.GetValue(item, null)).ToArray();
            var array = BuildArrowArray(field.DataType, values);
            arrays.Add(array);
        }
        return new RecordBatch(schema, arrays, items.Length);
    }
    private static Schema BuildSchema(Type type)
    {
        var builder = new Schema.Builder();

        foreach (var property in type.GetProperties())
        {
            var fieldType = InferArrowType(property.PropertyType);
            builder.Field(f => f.Name(property.Name).DataType(fieldType).Nullable(true));
        }

        return builder.Build();
    }
    
    private static IArrowType InferArrowType(Type type)
    {
        return type switch
        {
            { } t when t == typeof(string) => StringType.Default,
            { } t when t == typeof(int) => Int32Type.Default,
            { } t when t == typeof(float) => FloatType.Default,
            { } t when t == typeof(bool) => BooleanType.Default,
            { } t when t == typeof(long) => Int64Type.Default,
            _ => throw new NotSupportedException($"Unsupported type: {type}")
        };
    }
    
    private static IArrowArray BuildArrowArray(IArrowType dataType, object[] values, MemoryAllocator allocator = default)
    {
        allocator ??= MemoryAllocator.Default.Value;

        return dataType switch
        {
            StringType => BuildStringArray(values),
            Int32Type => BuildArray<int, Int32Array, Int32Array.Builder>(values, allocator),
            FloatType => BuildArray<float, FloatArray, FloatArray.Builder>(values, allocator),
            BooleanType => BuildArray<bool, BooleanArray, BooleanArray.Builder>(values, allocator),
            Int64Type => BuildArray<long, Int64Array, Int64Array.Builder>(values, allocator),
            _ => throw new NotSupportedException($"Unsupported Arrow type: {dataType}")
        };
    }
    
    private static IArrowArray BuildStringArray(object[] values)
    {
        var builder = new StringArray.Builder();

        foreach (var value in values)
        {
            if (value is null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(value.ToString());
            }
        }

        return builder.Build();
    }
    
    private static IArrowArray BuildArray<T, TArray, TBuilder>(object[] values, MemoryAllocator allocator)
        where TArray : IArrowArray
        where TBuilder : IArrowArrayBuilder<T, TArray, TBuilder>, new()
    {
        var builder = new TBuilder();

        foreach (var value in values)
        {
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((T)value);
            }
        }

        return builder.Build(allocator);
    }
}
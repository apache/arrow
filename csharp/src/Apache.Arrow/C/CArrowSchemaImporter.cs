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
using Apache.Arrow.Types;

namespace Apache.Arrow.C
{
    public sealed class CArrowSchemaImporter
    {
        /// <summary>
        /// Import C pointer as an <see cref="ArrowType"/>.
        /// </summary>
        /// <examples>
        /// Typically, you will allocate a uninitialized CArrowSchema pointer,
        /// pass that to external function, and then use this method to import
        /// the result.
        /// 
        /// <code>
        /// IntPtr importedPtr = CArrowSchema.AllocateUninitialized();
        /// foreign_export_function(importedPtr);
        /// ArrowType importedType = CArrowSchema.ImportType(importedPtr);
        /// CArrowSchema.FreePtr(importedPtr);
        /// </code>
        /// </examples>
        public static unsafe ArrowType ImportType(CArrowSchema* ptr)
        {
            using var importedType = new ImportedArrowSchema(ptr);
            return importedType.GetAsType();
        }

        /// <summary>
        /// Import C pointer as an <see cref="Field"/>.
        /// </summary>
        /// <examples>
        /// Typically, you will allocate a uninitialized CArrowSchema pointer,
        /// pass that to external function, and then use this method to import
        /// the result.
        /// 
        /// <code>
        /// IntPtr importedPtr = CArrowSchema.AllocateUninitialized();
        /// foreign_export_function(importedPtr);
        /// Field importedField = CArrowSchema.ImportField(importedPtr);
        /// CArrowSchema.FreePtr(importedPtr);
        /// </code>
        /// </examples>
        public static unsafe Field ImportField(CArrowSchema* ptr)
        {
            using var importedField = new ImportedArrowSchema(ptr);
            return importedField.GetAsField();
        }

        /// <summary>
        /// Import C pointer as an <see cref="Schema"/>.
        /// </summary>
        /// <examples>
        /// Typically, you will allocate a uninitialized CArrowSchema pointer,
        /// pass that to external function, and then use this method to import
        /// the result.
        /// 
        /// <code>
        /// IntPtr importedPtr = CArrowSchema.AllocateUninitialized();
        /// foreign_export_function(importedPtr);
        /// Field importedSchema = CArrowSchema.ImportSchema(importedPtr);
        /// CArrowSchema.FreePtr(importedPtr);
        /// </code>
        /// </examples>
        public static unsafe Schema ImportSchema(CArrowSchema* ptr)
        {
            using var importedSchema = new ImportedArrowSchema(ptr);
            return importedSchema.GetAsSchema();
        }

        private sealed unsafe class ImportedArrowSchema : IDisposable
        {
            private readonly CArrowSchema* _data;
            private readonly bool _isRoot;

            public ImportedArrowSchema(CArrowSchema* handle)
            {
                _data = (CArrowSchema*)handle;
                if (_data->release == null)
                {
                    throw new Exception("Tried to import a schema that has already been released.");
                }
                _isRoot = true;
            }

            private ImportedArrowSchema(CArrowSchema* handle, bool isRoot) : this(handle)
            {
                _isRoot = isRoot;
            }

            public void Dispose()
            {
                // We only call release on a root-level schema, not child ones.
                if (_isRoot && _data->release != null)
                {
                    _data->release(_data);
                }
            }

            public ArrowType GetAsType()
            {
                var format = StringUtil.PtrToStringUtf8(_data->format);
                if (_data->dictionary != null)
                {
                    ArrowType indicesType = format switch
                    {
                        "c" => new Int8Type(),
                        "C" => new UInt8Type(),
                        "s" => new Int16Type(),
                        "S" => new UInt16Type(),
                        "i" => new Int32Type(),
                        "I" => new UInt32Type(),
                        "l" => new Int64Type(),
                        "L" => new UInt64Type(),
                        _ => throw new InvalidDataException($"Indices must be an integer, but got format string {format}"),
                    };

                    var dictionarySchema = new ImportedArrowSchema(_data->dictionary, isRoot: false);
                    ArrowType dictionaryType = dictionarySchema.GetAsType();

                    bool ordered = _data->GetFlag(CArrowSchema.ArrowFlagDictionaryOrdered);

                    return new DictionaryType(indicesType, dictionaryType, ordered);
                }

                // Special handling for nested types
                if (format == "+l")
                {
                    if (_data->n_children != 1)
                    {
                        throw new Exception("Expected list type to have exactly one child.");
                    }
                    ImportedArrowSchema childSchema;
                    if (_data->GetChild(0) == null)
                    {
                        throw new Exception("Expected list type child to be non-null.");
                    }
                    childSchema = new ImportedArrowSchema(_data->GetChild(0), isRoot: false);

                    Field childField = childSchema.GetAsField();

                    return new ListType(childField);
                }
                else if (format == "+s")
                {
                    var child_schemas = new ImportedArrowSchema[_data->n_children];

                    for (int i = 0; i < _data->n_children; i++)
                    {
                        if (_data->GetChild(i) == null)
                        {
                            throw new Exception("Expected struct type child to be non-null.");
                        }
                        child_schemas[i] = new ImportedArrowSchema(_data->GetChild(i), isRoot: false);
                    }


                    List<Field> childFields = child_schemas.Select(schema => schema.GetAsField()).ToList();

                    return new StructType(childFields);
                }
                // TODO: Map type and large list type

                // Decimals
                if (format.StartsWith("d:"))
                {
                    bool is256 = format.EndsWith(",256");
                    string parameters_part = format.Remove(0, 2);
                    if (is256) parameters_part.Substring(0, parameters_part.Length - 5);
                    string[] parameters = parameters_part.Split(',');
                    int precision = Int32.Parse(parameters[0]);
                    int scale = Int32.Parse(parameters[1]);
                    if (is256)
                    {
                        return new Decimal256Type(precision, scale);
                    }
                    else
                    {
                        return new Decimal128Type(precision, scale);
                    }
                }

                // Timestamps
                if (format.StartsWith("ts"))
                {
                    TimeUnit timeUnit = format[2] switch
                    {
                        's' => TimeUnit.Second,
                        'm' => TimeUnit.Millisecond,
                        'u' => TimeUnit.Microsecond,
                        'n' => TimeUnit.Nanosecond,
                        _ => throw new InvalidDataException($"Unsupported time unit for import: {format[2]}"),
                    };

                    string timezone = format.Split(':')[1];
                    return new TimestampType(timeUnit, timezone);
                }

                // Fixed-width binary
                if (format.StartsWith("w:"))
                {
                    int width = Int32.Parse(format.Substring(2));
                    return new FixedSizeBinaryType(width);
                }

                return format switch
                {
                    // Primitives
                    "n" => new NullType(),
                    "b" => new BooleanType(),
                    "c" => new Int8Type(),
                    "C" => new UInt8Type(),
                    "s" => new Int16Type(),
                    "S" => new UInt16Type(),
                    "i" => new Int32Type(),
                    "I" => new UInt32Type(),
                    "l" => new Int64Type(),
                    "L" => new UInt64Type(),
                    "e" => new HalfFloatType(),
                    "f" => new FloatType(),
                    "g" => new DoubleType(),
                    // Binary data
                    "z" => new BinaryType(),
                    //"Z" => new LargeBinaryType() // Not yet implemented
                    "u" => new StringType(),
                    //"U" => new LargeStringType(), // Not yet implemented
                    // Date and time
                    "tdD" => new Date32Type(),
                    "tdm" => new Date64Type(),
                    "tts" => new Time32Type(TimeUnit.Second),
                    "ttm" => new Time32Type(TimeUnit.Millisecond),
                    "ttu" => new Time64Type(TimeUnit.Microsecond),
                    "ttn" => new Time64Type(TimeUnit.Nanosecond),
                    // TODO: duration not yet implemented
                    "tiM" => new IntervalType(IntervalUnit.YearMonth),
                    "tiD" => new IntervalType(IntervalUnit.DayTime),
                    //"tin" => new IntervalType(IntervalUnit.MonthDayNanosecond), // Not yet implemented
                    _ => throw new NotSupportedException("Data type is not yet supported in import.")
                };
            }

            public Field GetAsField()
            {
                string name = StringUtil.PtrToStringUtf8(_data->name);
                string fieldName = string.IsNullOrEmpty(name) ? "" : name;

                bool nullable = _data->GetFlag(CArrowSchema.ArrowFlagNullable);

                return new Field(fieldName, GetAsType(), nullable);
            }

            public Schema GetAsSchema()
            {
                ArrowType fullType = GetAsType();
                if (fullType is StructType structType)
                {
                    return new Schema(structType.Fields, default);
                }
                else
                {
                    throw new Exception("Imported type is not a struct type, so it cannot be converted to a schema.");
                }
            }
        }
    }
}
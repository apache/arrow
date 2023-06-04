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
using Apache.Arrow.Types;

namespace Apache.Arrow.C
{
    public static class CArrowSchemaImporter
    {
        /// <summary>
        /// Import C pointer as an <see cref="ArrowType"/>.
        /// </summary>
        /// <remarks>
        /// This will call the release callback on the passed struct, even if
        /// this function fails.
        /// </remarks>
        /// <examples>
        /// Typically, you will allocate an uninitialized CArrowSchema pointer,
        /// pass that to external function, and then use this method to import
        /// the result.
        /// 
        /// <code>
        /// CArrowSchema* importedPtr = CArrowSchema.Create();
        /// foreign_export_function(importedPtr);
        /// ArrowType importedType = CArrowSchemaImporter.ImportType(importedPtr);
        /// CArrowSchema.Free(importedPtr);
        /// </code>
        /// </examples>
        public static unsafe ArrowType ImportType(CArrowSchema* ptr)
        {
            using var importedType = new ImportedArrowSchema(ptr);
            return importedType.GetAsType();
        }

        /// <summary>
        /// Import C pointer as a <see cref="Field"/>.
        /// </summary>
        /// <remarks>
        /// This will call the release callback on the passed struct, even if
        /// this function fails.
        /// </remarks>
        /// <examples>
        /// Typically, you will allocate an uninitialized CArrowSchema pointer,
        /// pass that to external function, and then use this method to import
        /// the result.
        /// 
        /// <code>
        /// CArrowSchema* importedPtr = CArrowSchema.Create();
        /// foreign_export_function(importedPtr);
        /// Field importedField = CArrowSchemaImporter.ImportField(importedPtr);
        /// CArrowSchema.Free(importedPtr);
        /// </code>
        /// </examples>
        public static unsafe Field ImportField(CArrowSchema* ptr)
        {
            using var importedField = new ImportedArrowSchema(ptr);
            return importedField.GetAsField();
        }

        /// <summary>
        /// Import C pointer as a <see cref="Schema"/>.
        /// </summary>
        /// <remarks>
        /// This will call the release callback on the passed struct, even if
        /// this function fails.
        /// </remarks>
        /// <examples>
        /// Typically, you will allocate an uninitialized CArrowSchema pointer,
        /// pass that to external function, and then use this method to import
        /// the result.
        /// 
        /// <code>
        /// CArrowSchema* importedPtr = CArrowSchema.Create();
        /// foreign_export_function(importedPtr);
        /// Field importedSchema = CArrowSchemaImporter.ImportSchema(importedPtr);
        /// CArrowSchema.Free(importedPtr);
        /// </code>
        /// </examples>
        public static unsafe Schema ImportSchema(CArrowSchema* ptr)
        {
            using var importedSchema = new ImportedArrowSchema(ptr);
            return importedSchema.GetAsSchema();
        }

        private sealed unsafe class ImportedArrowSchema : IDisposable
        {
            private readonly CArrowSchema* _cSchema;
            private readonly bool _isRoot;

            public ImportedArrowSchema(CArrowSchema* cSchema)
            {
                if (cSchema == null)
                {
                    throw new ArgumentException("Passed null pointer for cSchema.");
                }
                _cSchema = cSchema;
                if (_cSchema->release == null)
                {
                    throw new ArgumentException("Tried to import a schema that has already been released.");
                }
                _isRoot = true;
            }

            public ImportedArrowSchema(CArrowSchema* handle, bool isRoot) : this(handle)
            {
                _isRoot = isRoot;
            }

            public void Dispose()
            {
                // We only call release on a root-level schema, not child ones.
                if (_isRoot && _cSchema->release != null)
                {
                    _cSchema->release(_cSchema);
                }
            }

            public ArrowType GetAsType()
            {
                var format = StringUtil.PtrToStringUtf8(_cSchema->format);
                if (_cSchema->dictionary != null)
                {
                    ArrowType indicesType = format switch
                    {
                        "c" => Int8Type.Default,
                        "C" => UInt8Type.Default,
                        "s" => Int16Type.Default,
                        "S" => UInt16Type.Default,
                        "i" => Int32Type.Default,
                        "I" => UInt32Type.Default,
                        "l" => Int64Type.Default,
                        "L" => UInt64Type.Default,
                        _ => throw new InvalidDataException($"Indices must be an integer, but got format string {format}"),
                    };

                    var dictionarySchema = new ImportedArrowSchema(_cSchema->dictionary, isRoot: false);
                    ArrowType dictionaryType = dictionarySchema.GetAsType();

                    bool ordered = _cSchema->GetFlag(CArrowSchema.ArrowFlagDictionaryOrdered);

                    return new DictionaryType(indicesType, dictionaryType, ordered);
                }

                // Special handling for nested types
                if (format == "+l")
                {
                    if (_cSchema->n_children != 1)
                    {
                        throw new InvalidDataException("Expected list type to have exactly one child.");
                    }
                    ImportedArrowSchema childSchema;
                    if (_cSchema->GetChild(0) == null)
                    {
                        throw new InvalidDataException("Expected list type child to be non-null.");
                    }
                    childSchema = new ImportedArrowSchema(_cSchema->GetChild(0), isRoot: false);

                    Field childField = childSchema.GetAsField();

                    return new ListType(childField);
                }
                else if (format == "+s")
                {
                    var child_schemas = new ImportedArrowSchema[_cSchema->n_children];

                    for (int i = 0; i < _cSchema->n_children; i++)
                    {
                        if (_cSchema->GetChild(i) == null)
                        {
                            throw new InvalidDataException("Expected struct type child to be non-null.");
                        }
                        child_schemas[i] = new ImportedArrowSchema(_cSchema->GetChild(i), isRoot: false);
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

                    string timezone = format.Substring(format.IndexOf(':') + 1);
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
                    "n" => NullType.Default,
                    "b" => BooleanType.Default,
                    "c" => Int8Type.Default,
                    "C" => UInt8Type.Default,
                    "s" => Int16Type.Default,
                    "S" => UInt16Type.Default,
                    "i" => Int32Type.Default,
                    "I" => UInt32Type.Default,
                    "l" => Int64Type.Default,
                    "L" => UInt64Type.Default,
                    "e" => HalfFloatType.Default,
                    "f" => FloatType.Default,
                    "g" => DoubleType.Default,
                    // Binary data
                    "z" => BinaryType.Default,
                    //"Z" => new LargeBinaryType() // Not yet implemented
                    "u" => StringType.Default,
                    //"U" => new LargeStringType(), // Not yet implemented
                    // Date and time
                    "tdD" => Date32Type.Default,
                    "tdm" => Date64Type.Default,
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
                string name = StringUtil.PtrToStringUtf8(_cSchema->name);
                string fieldName = string.IsNullOrEmpty(name) ? "" : name;

                bool nullable = _cSchema->GetFlag(CArrowSchema.ArrowFlagNullable);

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
                    throw new ArgumentException("Imported type is not a struct type, so it cannot be converted to a schema.");
                }
            }
        }
    }
}

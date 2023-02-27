// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Apache.Arrow.Types;

namespace Apache.Arrow.C
{
    /// <summary>
    /// A <see cref="CArrowSchema"/> imported from somewhere else.
    /// </summary>
    ///
    /// <example>
    /// Typically, when importing a schema we will allocate an uninitialized 
    /// <see cref="CArrowSchema"/>, pass the pointer to the foreign function,
    /// then construct this class with the initialized pointer.
    /// 
    /// <code>
    /// var cSchema = new CArrowSchema();
    /// IntPtr importedPtr = cSchema.AllocateAsPtr();
    /// foreign_export_function(importedPtr);
    /// var importedType = new ImportedArrowSchema(importedPtr);
    /// ArrowType arrowType = importedType.GetAsType();
    /// <code>
    /// </example>
    public sealed class ImportedArrowSchema : IDisposable
    {
        private readonly CArrowSchema _data;
        private readonly IntPtr _handle;
        private readonly bool _isRoot;

        public ImportedArrowSchema(IntPtr handle)
        {
            _data = Marshal.PtrToStructure<CArrowSchema>(handle);
            if (_data.release == null)
            {
                throw new Exception("Tried to import a schema that has already been released.");
            }
            _handle = handle;
            _isRoot = true;
        }

        private ImportedArrowSchema(IntPtr handle, bool isRoot) : this(handle)
        {
            _isRoot = isRoot;
        }

        public void Dispose()
        {
            // We only call release on a root-level schema, not child ones.
            if (_isRoot)
            {
                _data.release(_handle);
            }
        }

        public ArrowType GetAsType()
        {
            if (_data.dictionary != IntPtr.Zero)
            {
                ArrowType indices_type = _data.format switch
                {
                    "c" => new Int8Type(),
                    "C" => new UInt8Type(),
                    "s" => new Int16Type(),
                    "S" => new UInt16Type(),
                    "i" => new Int32Type(),
                    "I" => new UInt32Type(),
                    "l" => new Int64Type(),
                    "L" => new UInt64Type(),
                    _ => throw new InvalidDataException($"Indices must be an integer, but got format string {_data.format}"),
                };

                var dictionarySchema = new ImportedArrowSchema(_data.dictionary, /*is_root*/ false);
                ArrowType dictionaryType = dictionarySchema.GetAsType();

                bool ordered = (_data.flags & CArrowSchema.ArrowFlagNullable) == CArrowSchema.ArrowFlagNullable;

                return new DictionaryType(indices_type, dictionaryType, ordered);
            }

            // Special handling for nested types
            if (_data.format == "+l")
            {
                if (_data.n_children != 1)
                {
                    throw new Exception("Expected list type to have exactly one child.");
                }
                ImportedArrowSchema childSchema;
                unsafe
                {
                    if (_data.children[0] == null)
                    {
                        throw new Exception("Expected list type child to be non-null.");
                    }
                    childSchema = new ImportedArrowSchema(_data.children[0]);
                }

                Field childField = childSchema.GetAsField();

                return new ListType(childField);
            }
            else if (_data.format == "+s")
            {
                var child_schemas = new ImportedArrowSchema[_data.n_children];
                unsafe
                {
                    for (int i = 0; i < _data.n_children; i++)
                    {
                        if (_data.children[i] == null)
                        {
                            throw new Exception("Expected struct type child to be non-null.");
                        }
                        child_schemas[i] = new ImportedArrowSchema(_data.children[i]);
                    }

                }

                List<Field> childFields = child_schemas.Select(schema => schema.GetAsField()).ToList();

                return new StructType(childFields);
            }
            // TODO: Map type and large list type

            return _data.format switch
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
                // TODO: decimal
                // TODO: fixed-width binary
                // Date and time
                "tdD" => new Date32Type(),
                "tdm" => new Date64Type(),
                "tts" => new Time32Type(TimeUnit.Second),
                "ttm" => new Time32Type(TimeUnit.Millisecond),
                "ttu" => new Time64Type(TimeUnit.Microsecond),
                "ttn" => new Time64Type(TimeUnit.Nanosecond),
                // TODO: timestamp with timezone,
                // TODO: duration not yet implemented
                "tiM" => new IntervalType(IntervalUnit.YearMonth),
                "tiD" => new IntervalType(IntervalUnit.DayTime),
                //"tin" => new IntervalType(IntervalUnit.MonthDayNanosecond), // Not yet implemented
                _ => throw new NotSupportedException("Data type is not yet supported in import.")
            };
        }

        public Field GetAsField()
        {
            string fieldName = string.IsNullOrEmpty(_data.name) ? "" : _data.name;

            bool nullable = (_data.flags & CArrowSchema.ArrowFlagNullable) == CArrowSchema.ArrowFlagNullable;

            return new Field(fieldName, GetAsType(), nullable);
        }

        public Schema GetAsSchema()
        {
            ArrowType fullType = GetAsType();
            if (fullType is StructType)
            {
                StructType structType = (StructType)fullType;
                return new Schema(structType.Fields, default);
            }
            else
            {
                throw new Exception("Imported type is not a struct type, so it cannot be converted to a schema.");
            }
        }
    }
}

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

[UnmanagedFunctionPointer(CallingConvention.StdCall)]
public delegate void ReleaseCArrowSchema(IntPtr schema);

namespace Apache.Arrow.C
{
    /// <summary>
    /// An Arrow C Data Interface Schema, which represents a type, field, or schema.
    /// </summary>
    /// 
    /// <remarks>
    /// This is used to export <see cref="ArrowType"/>, <see cref="Field"/>, or
    /// <see cref="Schema"/> to other languages. It matches the layout of the
    /// ArrowSchema struct described in https://github.com/apache/arrow/blob/main/cpp/src/arrow/c/abi.h.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    public struct CArrowSchema
    {
        public IntPtr format;
        public IntPtr name;
        public IntPtr metadata;
        public long flags;
        public long n_children;
        public IntPtr children;
        public IntPtr dictionary;
        [MarshalAs(UnmanagedType.FunctionPtr)]
        public ReleaseCArrowSchema release;
        public IntPtr private_data;

        private static char FormatTimeUnit(TimeUnit unit) => unit switch
        {
            TimeUnit.Second => 's',
            TimeUnit.Millisecond => 'm',
            TimeUnit.Microsecond => 'u',
            TimeUnit.Nanosecond => 'n',
            _ => throw new InvalidDataException($"Unsupported time unit for export: {unit}"),
        };

        private static string GetFormat(IArrowType datatype)
        {
            switch (datatype)
            {
                case NullType _: return "n";
                case BooleanType _: return "b";
                // Integers
                case Int8Type _: return "c";
                case UInt8Type _: return "C";
                case Int16Type _: return "s";
                case UInt16Type _: return "S";
                case Int32Type _: return "i";
                case UInt32Type _: return "I";
                case Int64Type _: return "l";
                case UInt64Type _: return "L";
                // Floats
                case HalfFloatType _: return "e";
                case FloatType _: return "f";
                case DoubleType _: return "g";
                // Decimal
                case Decimal128Type decimalType:
                    return $"d:{decimalType.Precision},{decimalType.Scale}";
                case Decimal256Type decimalType:
                    return $"d:{decimalType.Precision},{decimalType.Scale},256";
                // Binary
                case BinaryType _: return "z";
                case StringType _: return "u";
                case FixedSizeBinaryType binaryType:
                    return $"w:{binaryType.ByteWidth}";
                // Date
                case Date32Type _: return "tdD";
                case Date64Type _: return "tdm";
                // Time
                case Time32Type timeType:
                    return String.Format("tt{0}", FormatTimeUnit(timeType.Unit));
                case Time64Type timeType:
                    // Same prefix as Time32, but allowed time units are different.
                    return String.Format("tt{0}", FormatTimeUnit(timeType.Unit));
                // Timestamp
                case TimestampType timestampType:
                    return String.Format("ts{0}:{1}", FormatTimeUnit(timestampType.Unit), timestampType.Timezone);
                // Nested
                case ListType _: return "+l";
                case StructType _: return "+s";
                // Dictionary
                case DictionaryType dictionaryType:
                    return GetFormat(dictionaryType.IndexType);
                default: throw new NotImplementedException($"Exporting {datatype.Name} not implemented");
            };
        }

        private static long GetFlags(IArrowType datatype, bool nullable = true)
        {
            long flags = 0;

            if (nullable)
            {
                flags |= ArrowFlagNullable;
            }

            if (datatype is DictionaryType dictionaryType)
            {
                if (dictionaryType.Ordered)
                {
                    flags |= ArrowFlagDictionaryOrdered;
                }
            }

            // TODO: when we implement MapType, make sure to set the KEYS_SORTED flag.
            return flags;
        }

        /// <summary>
        /// Whether this field is semantically nullable (regardless of whether it actually has null values)
        /// </summary>
        public const long ArrowFlagDictionaryOrdered = 1;
        /// <summary>
        /// For dictionary-encoded types, whether the ordering of dictionary indices is semantically meaningful.
        /// </summary>
        public const long ArrowFlagNullable = 2;
        /// <summary>
        /// For map types, whether the keys within each map value are sorted.
        /// </summary>
        public const long ArrowFlagMapKeysSorted = 4;

        /// <summary>
        /// Get the value of a particular flag.
        /// </summary>
        /// <remarks>
        /// Known valid flags are <see cref="ArrowFlagDictionaryOrdered" />,
        /// <see cref="ArrowFlagNullable" />, and <see cref="ArrowFlagMapKeysSorted" />.
        /// </remarks>
        public bool GetFlag(long flag)
        {
            return (flags & flag) == flag;
        }

        private static IntPtr ConstructChildren(IArrowType datatype, out long numChildren)
        {
            if (datatype is NestedType nestedType)
            {
                IReadOnlyList<Field> fields = nestedType.Fields;
                int numFields = fields.Count;
                numChildren = numFields;

                unsafe
                {
                    IntPtr* pointerList = (IntPtr*)Marshal.AllocHGlobal(numFields * IntPtr.Size);

                    for (var i = 0; i < numChildren; i++)
                    {
                        var cSchema = new CArrowSchema(fields[i]);
                        IntPtr exportedSchema = cSchema.AllocateAsPtr();
                        pointerList[i] = exportedSchema;
                    }

                    return (IntPtr)pointerList;
                }

            }
            else
            {
                numChildren = 0;
                return IntPtr.Zero;
            }
        }

        private IntPtr GetChild(int i)
        {
            if (i >= n_children)
            {
                throw new Exception("Child index out of bounds.");
            }
            if (children == IntPtr.Zero)
            {
                throw new Exception("Children array is null.");
            }
            unsafe
            {
                return ((IntPtr*)children)[i];
            }
        }

        private static IntPtr ConstructDictionary(IArrowType datatype)
        {
            if (datatype is DictionaryType dictType)
            {
                var cSchema = new CArrowSchema(dictType.ValueType);
                return cSchema.AllocateAsPtr();
            }
            else
            {
                return IntPtr.Zero;
            }
        }

        /// <summary>
        /// Initialize the exported C schema as an Arrow type.
        /// </summary>
        /// <param name="datatype">The Arrow type to export.</param>
        public CArrowSchema(IArrowType datatype)
        {
            format = StringUtil.ToCStringUtf8(GetFormat(datatype));
            name = IntPtr.Zero;
            metadata = IntPtr.Zero;
            flags = GetFlags(datatype);

            children = ConstructChildren(datatype, out var numChildren);
            n_children = numChildren;

            dictionary = ConstructDictionary(datatype);

            release = (IntPtr self) =>
            {
                var schema = Marshal.PtrToStructure<CArrowSchema>(self);

                Marshal.FreeHGlobal(schema.format);
                Marshal.FreeHGlobal(schema.name);
                Marshal.FreeHGlobal(schema.metadata);

                if (schema.n_children > 0)
                {
                    for (int i = 0; i < schema.n_children; i++)
                    {
                        FreePtr(schema.GetChild(i));
                    }
                    Marshal.FreeHGlobal((IntPtr)schema.children);
                }

                if (schema.dictionary != IntPtr.Zero)
                {
                    FreePtr(schema.dictionary);
                }
                Marshal.DestroyStructure<CArrowSchema>(self);
            };

            private_data = IntPtr.Zero;
        }

        /// <summary>
        /// Initialize the exported C schema as a field.
        /// </summary>
        /// <param name="field">Field to export.</param>
        public CArrowSchema(Field field) : this(field.DataType)
        {
            name = StringUtil.ToCStringUtf8(field.Name);
            // TODO: field metadata
            metadata = IntPtr.Zero;
            flags = GetFlags(field.DataType, field.IsNullable);
        }

        /// <summary>
        /// Initialize the exported C schema as a schema.
        /// </summary>
        /// <param name="schema">Schema to export.</param>
        public CArrowSchema(Schema schema) : this(new StructType(schema.Fields.Values.ToList()))
        {
            // TODO: top-level metadata
        }

        /// <summary>
        /// Allocate an unmanaged pointer and copy this instances data to it.
        /// </summary>
        /// <remarks>
        /// To avoid a memory leak, you must call <see cref="FreePtr"/> on this
        /// pointer when done using it.
        /// </remarks>
        public IntPtr AllocateAsPtr()
        {
            IntPtr ptr = Marshal.AllocHGlobal(Marshal.SizeOf(this));
            Marshal.StructureToPtr<CArrowSchema>(this, ptr, false);
            return ptr;
        }

        /// <summary>
        /// Free a pointer that was allocated in <see cref="AllocateAsPtr"/>.
        /// </summary>
        /// <remarks>
        /// Do not call this on a pointer that was allocated elsewhere.
        /// </remarks>
        public static void FreePtr(IntPtr ptr)
        {
            var schema = Marshal.PtrToStructure<CArrowSchema>(ptr);
            if (schema.release != null)
            {
                // Call release if not already called.
                schema.release(ptr);
            }
            Marshal.FreeHGlobal(ptr);
        }

        /// <summary>
        /// Export to an existing pointer
        /// </summary>
        /// <param name="ptr">An allocated but uninitialized pointer.</param>
        public void Export(IntPtr ptr)
        {
            var schema = Marshal.PtrToStructure<CArrowSchema>(ptr);
            if (schema.release != null)
            {
                throw new Exception("Cannot export to a ArrowSchema pointer is that is already initialized.");
            }
            Marshal.StructureToPtr<CArrowSchema>(this, ptr, false);
        }

        /// <summary>
        /// Allocated a new pointer to an uninitialized CArrowSchema.
        /// </summary>
        /// <remarks>This is used to import schemas. See  <see cref="ImportType"/>,
        /// <see cref="ImportField"/>, and <see cref="ImportSchema"/>. Once data 
        /// is imported, this pointer must be cleaned up with <see cref="FreePtr"/>.
        /// </remarks>
        public static IntPtr AllocateUninitialized()
        {
            return Marshal.AllocHGlobal(Marshal.SizeOf<CArrowSchema>());
        }

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
        public static ArrowType ImportType(IntPtr ptr)
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
        public static Field ImportField(IntPtr ptr)
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
        public static Schema ImportSchema(IntPtr ptr)
        {
            using var importedSchema = new ImportedArrowSchema(ptr);
            return importedSchema.GetAsSchema();
        }

        private sealed class ImportedArrowSchema : IDisposable
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
                if (_isRoot && _data.release != null)
                {
                    _data.release(_handle);
                }
            }

            public ArrowType GetAsType()
            {
                var format = StringUtil.PtrToStringUtf8(_data.format);
                if (_data.dictionary != IntPtr.Zero)
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

                    var dictionarySchema = new ImportedArrowSchema(_data.dictionary, isRoot: false);
                    ArrowType dictionaryType = dictionarySchema.GetAsType();

                    bool ordered = _data.GetFlag(CArrowSchema.ArrowFlagDictionaryOrdered);

                    return new DictionaryType(indicesType, dictionaryType, ordered);
                }

                // Special handling for nested types
                if (format == "+l")
                {
                    if (_data.n_children != 1)
                    {
                        throw new Exception("Expected list type to have exactly one child.");
                    }
                    ImportedArrowSchema childSchema;
                    if (_data.GetChild(0) == IntPtr.Zero)
                    {
                        throw new Exception("Expected list type child to be non-null.");
                    }
                    childSchema = new ImportedArrowSchema(_data.GetChild(0), isRoot: false);

                    Field childField = childSchema.GetAsField();

                    return new ListType(childField);
                }
                else if (format == "+s")
                {
                    var child_schemas = new ImportedArrowSchema[_data.n_children];

                    for (int i = 0; i < _data.n_children; i++)
                    {
                        if (_data.GetChild(i) == IntPtr.Zero)
                        {
                            throw new Exception("Expected struct type child to be non-null.");
                        }
                        child_schemas[i] = new ImportedArrowSchema(_data.GetChild(i), isRoot: false);
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
                string name = StringUtil.PtrToStringUtf8(_data.name);
                string fieldName = string.IsNullOrEmpty(name) ? "" : name;

                bool nullable = _data.GetFlag(CArrowSchema.ArrowFlagNullable);

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

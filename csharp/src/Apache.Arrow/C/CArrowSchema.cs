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
    unsafe public struct CArrowSchema
    {
        public IntPtr format;
        public IntPtr name;
        public IntPtr metadata;
        public long flags;
        public long n_children;
        public IntPtr* children;
        public IntPtr dictionary;
        [MarshalAs(UnmanagedType.FunctionPtr)]
        public ReleaseCArrowSchema release;
        public IntPtr private_data;

        private static string GetFormat(IArrowType datatype)
        {
            TypeFormatter formatter = new TypeFormatter();
            datatype.Accept(formatter);
            return formatter.FormatString;
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

        private static IntPtr* ConstructChildren(IArrowType datatype, out long numChildren)
        {
            if (datatype is NestedType nestedType)
            {
                IReadOnlyList<Field> fields = nestedType.Fields;
                int numFields = fields.Count;
                numChildren = numFields;

                IntPtr* pointerList = (IntPtr*)Marshal.AllocHGlobal(numFields * sizeof(IntPtr));

                for (var i = 0; i < numChildren; i++)
                {
                    var cSchema = new CArrowSchema(fields[i]);
                    IntPtr exportedSchema = cSchema.AllocateAsPtr();
                    pointerList[i] = exportedSchema;
                }

                return pointerList;
            }
            else
            {
                numChildren = 0;
                return (IntPtr*)IntPtr.Zero;
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
                        FreePtr(schema.children[i]);
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
            Marshal.StructureToPtr<CArrowSchema>(this, ptr, false);
        }

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
        /// </code>
        /// </examples>
        public static Schema ImportSchema(IntPtr ptr)
        {
            using var importedSchema = new ImportedArrowSchema(ptr);
            return importedSchema.GetAsSchema();
        }

        public const int ArrowFlagDictionaryOrdered = 1;
        public const int ArrowFlagNullable = 2;
        public const int ArrowFlagMapKeysSorted = 4;

        private class TypeFormatter :
        IArrowTypeVisitor<NullType>,
        IArrowTypeVisitor<BooleanType>,
        IArrowTypeVisitor<Int8Type>,
        IArrowTypeVisitor<Int16Type>,
        IArrowTypeVisitor<Int32Type>,
        IArrowTypeVisitor<Int64Type>,
        IArrowTypeVisitor<UInt8Type>,
        IArrowTypeVisitor<UInt16Type>,
        IArrowTypeVisitor<UInt32Type>,
        IArrowTypeVisitor<UInt64Type>,
        IArrowTypeVisitor<HalfFloatType>,
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
        IArrowTypeVisitor<StructType>,
        IArrowTypeVisitor<DictionaryType>
        {
            public string FormatString;
            public void Visit(NullType _) => FormatString = "n";
            public void Visit(BooleanType _) => FormatString = "b";
            // Integers
            public void Visit(Int8Type _) => FormatString = "c";
            public void Visit(UInt8Type _) => FormatString = "C";
            public void Visit(Int16Type _) => FormatString = "s";
            public void Visit(UInt16Type _) => FormatString = "S";
            public void Visit(Int32Type _) => FormatString = "i";
            public void Visit(UInt32Type _) => FormatString = "I";
            public void Visit(Int64Type _) => FormatString = "l";
            public void Visit(UInt64Type _) => FormatString = "L";
            // Floats
            public void Visit(HalfFloatType _) => FormatString = "e";
            public void Visit(FloatType _) => FormatString = "f";
            public void Visit(DoubleType _) => FormatString = "g";
            // Binary
            public void Visit(BinaryType _) => FormatString = "z";
            public void Visit(StringType _) => FormatString = "u";
            public void Visit(FixedSizeBinaryType datatype)
            {
                FormatString = $"w:{datatype.ByteWidth}";
            }
            // Decimal
            public void Visit(Decimal128Type datatype)
            {
                FormatString = $"d:{datatype.Precision},{datatype.Scale}";
            }
            public void Visit(Decimal256Type datatype)
            {
                FormatString = $"w:{datatype.Precision},{datatype.Scale},256";
            }
            // Date
            public void Visit(Date32Type _) => FormatString = "tdD";
            public void Visit(Date64Type _) => FormatString = "tdm";

            private char TimeUnitComponent(TimeUnit unit) => unit switch
            {
                TimeUnit.Second => 's',
                TimeUnit.Millisecond => 'm',
                TimeUnit.Microsecond => 'u',
                TimeUnit.Nanosecond => 'n',
                _ => throw new InvalidDataException($"Unsupported time unit for export: {unit}"),
            };
            // Time
            public void Visit(Time32Type datatype)
            {
                FormatString = String.Format("tt{0}", TimeUnitComponent(datatype.Unit));
            }
            public void Visit(Time64Type datatype)
            {
                FormatString = String.Format("tt{0}", TimeUnitComponent(datatype.Unit));
            }
            // Timestamp type
            public void Visit(TimestampType datatype)
            {
                FormatString = String.Format("ts{0}:{1}", TimeUnitComponent(datatype.Unit), datatype.Timezone);
            }
            // Nested
            public void Visit(ListType _) => FormatString = "+l";
            public void Visit(StructType _) => FormatString = "+s";
            // Dictionary
            public void Visit(DictionaryType datatype)
            {
                // format string is that of the indices
                datatype.IndexType.Accept(this);
            }

            // Unsupported
            public void Visit(IArrowType type)
            {
                throw new NotImplementedException($"Exporting {type.Name} not implemented");
            }
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
                if (_isRoot)
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

                    var dictionarySchema = new ImportedArrowSchema(_data.dictionary, /*is_root*/ false);
                    ArrowType dictionaryType = dictionarySchema.GetAsType();

                    bool ordered = (_data.flags & CArrowSchema.ArrowFlagNullable) == CArrowSchema.ArrowFlagNullable;

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
                else if (format == "+s")
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
                string name = StringUtil.PtrToStringUtf8(_data.name);
                string fieldName = string.IsNullOrEmpty(name) ? "" : name;

                bool nullable = (_data.flags & CArrowSchema.ArrowFlagNullable) == CArrowSchema.ArrowFlagNullable;

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

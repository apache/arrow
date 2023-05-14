using System;
using System.Text;
using Apache.Arrow.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    // Inspired from C++ implementation
    // https://arrow.apache.org/docs/cpp/api/scalar.html
    public struct NullableScalar : INullableScalar
    {
        public IScalar Value { get; }
        public bool IsValid => Value != null;
        public IArrowType Type { get; }

        public NullableScalar(IArrowType type, IScalar value)
        {
            Type = type;
            Value = value;
        }
    }

    public struct BinaryScalar : IBaseBinaryScalar<BinaryType>
    {
        public ArrowBuffer Buffer { get; }
        public BinaryType Type => BinaryType.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public BinaryScalar(ArrowBuffer buffer)
        {
            Buffer = buffer;
        }

        public unsafe ReadOnlySpan<byte> AsBytes() => Buffer.Span;
    }

    public struct StringScalar : IBaseBinaryScalar<StringType>
    {
        public ArrowBuffer Buffer { get; }
        public StringType Type => StringType.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;
        private int _byteLength;

        public StringScalar(string value)
            : this(new ArrowBuffer(StringType.DefaultEncoding.GetBytes(value)))
        {
        }

        internal StringScalar(ArrowBuffer value)
        {
            Buffer = value;
            _byteLength = value.Length;
        }

        public string Value => GetString(StringType.DefaultEncoding);
        public unsafe ReadOnlySpan<byte> AsBytes() => Buffer.Span;

        /// <summary>
        /// Converts the underlying byte data to a string using the specified encoding.
        /// </summary>
        /// <remarks>
        /// Since strings are not nullable in a StringScalar, if the byte data is empty, it returns <see cref="string.Empty"/>.
        /// </remarks>
        /// <param name="encoding">The encoding used to decode the byte data.</param>
        /// <returns>String representation of the decoded byte data.</returns>
        public unsafe string GetString(Encoding encoding)
        {
            fixed (byte* ptr = AsBytes())
            {
                if (ptr == null)
                    return string.Empty;
                int charLength = encoding.GetCharCount(ptr, _byteLength);
                char[] buffer = new char[charLength];

                fixed (char* bufferPtr = buffer)
                {
                    encoding.GetChars(ptr, _byteLength, bufferPtr, charLength);
                }

                return new string(buffer);
            }
        }
    }

#pragma warning disable CS9084 // Struct member returns 'this' or other instance members by reference
    public struct BooleanScalar : IPrimitiveScalar<BooleanType>, IDotNetStruct<bool>
    {
        private bool _value;
        public bool DotNet => _value;
        public BooleanType Type => BooleanType.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public BooleanScalar(bool value)
        {
            _value = value;
        }
        public unsafe ReadOnlySpan<byte> AsBytes() => TypeReflection.AsBytes(ref _value);
    }

    // Numeric scalars
    public struct UInt8Scalar : INumericScalar<UInt8Type>, IDotNetStruct<byte>
    {
        private byte _value;
        public byte DotNet => _value;
        public UInt8Type Type => UInt8Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public UInt8Scalar(byte value)
        {
            _value = value;
        }

        internal UInt8Scalar(ArrowBuffer value) : this(TypeReflection.CastTo<byte>(value.Span))
        {
        }

        public unsafe ReadOnlySpan<byte> AsBytes() => TypeReflection.AsBytes(ref _value);
    }

    public struct Int8Scalar : INumericScalar<Int8Type>, IDotNetStruct<sbyte>
    {
        private sbyte _value;
        public sbyte DotNet => _value;
        public Int8Type Type => Int8Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public Int8Scalar(sbyte value)
        {
            _value = value;
        }

        internal Int8Scalar(ArrowBuffer value) : this(TypeReflection.CastTo<sbyte>(value.Span))
        {
        }

        public unsafe ReadOnlySpan<byte> AsBytes() => TypeReflection.AsBytes(ref _value);
    }

    public struct UInt16Scalar : INumericScalar<UInt16Type>, IDotNetStruct<ushort>
    {
        private ushort _value;
        public ushort DotNet => _value;
        public UInt16Type Type => UInt16Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public UInt16Scalar(ushort value)
        {
            _value = value;
        }

        internal UInt16Scalar(ArrowBuffer value) : this(TypeReflection.CastTo<ushort>(value.Span))
        {
        }

        public unsafe ReadOnlySpan<byte> AsBytes() => TypeReflection.AsBytes(ref _value);
    }
    public struct Int16Scalar : INumericScalar<Int16Type>, IDotNetStruct<short>
    {
        private short _value;
        public short DotNet => _value;
        public Int16Type Type => Int16Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public Int16Scalar(short value)
        {
            _value = value;
        }

        internal Int16Scalar(ArrowBuffer value) : this(TypeReflection.CastTo<short>(value.Span))
        {
        }

        public unsafe ReadOnlySpan<byte> AsBytes() => TypeReflection.AsBytes(ref _value);
    }

    public struct UInt32Scalar : INumericScalar<UInt32Type>, IDotNetStruct<uint>
    {
        private uint _value;
        public uint DotNet => _value;
        public UInt32Type Type => UInt32Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public UInt32Scalar(uint value)
        {
            _value = value;
        }

        internal UInt32Scalar(ArrowBuffer value) : this(TypeReflection.CastTo<uint>(value.Span))
        {
        }

        public unsafe ReadOnlySpan<byte> AsBytes() => TypeReflection.AsBytes(ref _value);
    }

    public struct Int32Scalar : INumericScalar<Int32Type>, IDotNetStruct<int>
    {
        private int _value;
        public int DotNet => _value;
        public Int32Type Type => Int32Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public Int32Scalar(int value)
        {
            _value = value;
        }

        internal Int32Scalar(ArrowBuffer value) : this(TypeReflection.CastTo<int>(value.Span))
        {
        }

        public unsafe ReadOnlySpan<byte> AsBytes() => TypeReflection.AsBytes(ref _value);
    }

    public struct UInt64Scalar : INumericScalar<UInt64Type>, IDotNetStruct<ulong>
    {
        private ulong _value;
        public ulong DotNet => _value;
        public UInt64Type Type => UInt64Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public UInt64Scalar(ulong value)
        {
            _value = value;
        }

        internal UInt64Scalar(ArrowBuffer value) : this(TypeReflection.CastTo<ulong>(value.Span))
        {
        }

        public unsafe ReadOnlySpan<byte> AsBytes() => TypeReflection.AsBytes(ref _value);
    }

    public struct Int64Scalar : INumericScalar<Int64Type>, IDotNetStruct<long>
    {
        private long _value;
        public long DotNet => _value;
        public Int64Type Type => Int64Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public Int64Scalar(long value)
        {
            _value = value;
        }

        internal Int64Scalar(ArrowBuffer value) : this(TypeReflection.CastTo<long>(value.Span))
        {
        }

        public unsafe ReadOnlySpan<byte> AsBytes() => TypeReflection.AsBytes(ref _value);
    }

    public struct FloatScalar : INumericScalar<FloatType>, IDotNetStruct<float>
    {
        private float _value;
        public float DotNet => _value;
        public FloatType Type => FloatType.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public FloatScalar(float value)
        {
            _value = value;
        }

        internal FloatScalar(ArrowBuffer value) : this(TypeReflection.CastTo<float>(value.Span))
        {
        }

        public unsafe ReadOnlySpan<byte> AsBytes() => TypeReflection.AsBytes(ref _value);
    }

    public struct DoubleScalar : INumericScalar<DoubleType>, IDotNetStruct<double>
    {
        private double _value;
        public double DotNet => _value;
        public DoubleType Type => DoubleType.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public DoubleScalar(double value)
        {
            _value = value;
        }

        internal DoubleScalar(ArrowBuffer value) : this(TypeReflection.CastTo<double>(value.Span))
        {
        }

        public unsafe ReadOnlySpan<byte> AsBytes() => TypeReflection.AsBytes(ref _value);
    }

#if NET5_0_OR_GREATER
    public struct HalfFloatScalar : INumericScalar<HalfFloatType>, IDotNetStruct<Half>
    {
        private Half _value;
        public Half DotNet => _value;
        public HalfFloatType Type => HalfFloatType.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public HalfFloatScalar(Half value)
        {
            _value = value;
        }

        internal HalfFloatScalar(ArrowBuffer value) : this(TypeReflection.CastTo<Half>(value.Span))
        {
        }

        public unsafe ReadOnlySpan<byte> AsBytes() => TypeReflection.AsBytes(ref _value);
    }
#endif
#pragma warning restore CS9084 // Struct member returns 'this' or other instance members by reference

    public struct Decimal128Scalar : INumericScalar<Decimal128Type>, IDotNetStruct<decimal>
    {
        public ArrowBuffer Buffer { get; }
        public Decimal128Type Type { get; }
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public Decimal128Scalar(decimal value)
            : this(Decimal128Type.Default, value)
        {
        }

        public Decimal128Scalar(Decimal128Type type, decimal value)
            : this(type, new ArrowBuffer(TypeReflection.AsMemoryBytes(value, type)))
        {
        }

        internal Decimal128Scalar(Decimal128Type type, ArrowBuffer value)
        {
            Type = type;
            Buffer = value;
        }

        public decimal DotNet => DecimalUtility.GetDecimal(Buffer, 0, Type.Scale, Type.ByteWidth);
        public unsafe ReadOnlySpan<byte> AsBytes() => Buffer.Span;
    }

    public struct Decimal256Scalar : IDecimalScalar<Decimal256Type>, IDotNetStruct<decimal>
    {
        public ArrowBuffer Buffer { get; }
        public Decimal256Type Type { get; }
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public Decimal256Scalar(decimal value)
            : this(Decimal256Type.SystemDefault, value)
        {
        }

        public Decimal256Scalar(Decimal256Type type, decimal value)
            : this(type, new ArrowBuffer(TypeReflection.AsMemoryBytes(value, type)))
        {
        }

        internal Decimal256Scalar(Decimal256Type type, ArrowBuffer value)
        {
            Type = type;
            Buffer = value;
        }

        public decimal DotNet => DecimalUtility.GetDecimal(Buffer, 0, Type.Scale, Type.ByteWidth);
        public unsafe ReadOnlySpan<byte> AsBytes() => Buffer.Span;
    }

    // Nested scalars
    public struct ListScalar : IBaseListScalar<ListType>
    {
        public ListType Type { get; }
        IArrowType IScalar.Type => Type;
        public IArrowArray Array { get; }
        public bool IsValid { get; }

        public ListScalar(ListType type, IArrowArray array, bool isValid = true)
        {
            Type = type;
            Array = array;
            IsValid = isValid;
        }
    }

    public struct StructScalar : IStructScalar<StructType>
    {
        public StructType Type { get; }
        IArrowType IScalar.Type => Type;
        public IScalar[] Fields { get; }
        public bool IsValid { get; }

        public StructScalar(StructType type, IScalar[] fields, bool isValid = true)
        {
            Type = type;
            Fields = fields;
            IsValid = isValid;
        }
    }
}

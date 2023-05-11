using System;
using System.Text;
using Apache.Arrow.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    // Inspired from C++ implementation
    // https://arrow.apache.org/docs/cpp/api/scalar.html
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

        public ReadOnlySpan<byte> View() => Buffer.Span;
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

        public string Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        Encoding encoding = StringType.DefaultEncoding;

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
        }
        public ReadOnlySpan<byte> View() => Buffer.Span;
    }

    public struct BooleanScalar : IPrimitiveScalar<BooleanType>, IDotNetStruct<bool>
    {
        public ArrowBuffer Buffer => ArrowBuffer.Empty;
        public BooleanType Type => BooleanType.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;
        public bool Value { get; }

        public BooleanScalar(bool value)
        {
            Value = value;
        }
        public ReadOnlySpan<byte> View() => Buffer.Span;
    }

    // Numeric scalars
    public struct UInt8Scalar : INumericScalar<UInt8Type>, IDotNetStruct<byte>
    {
        public ArrowBuffer Buffer { get; }
        public UInt8Type Type => UInt8Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public UInt8Scalar(byte value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        internal UInt8Scalar(ArrowBuffer value)
        {
            Buffer = value;
        }

        public byte Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        return *ptr;
                    }
                }
            }
        }
        public ReadOnlySpan<byte> View() => Buffer.Span;
    }

    public struct Int8Scalar : INumericScalar<Int8Type>, IDotNetStruct<sbyte>
    {
        public ArrowBuffer Buffer { get; }
        public Int8Type Type => Int8Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public Int8Scalar(sbyte value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        internal Int8Scalar(ArrowBuffer value)
        {
            Buffer = value;
        }

        public sbyte Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        return *(sbyte*)ptr;
                    }
                }
            }
        }
        public ReadOnlySpan<byte> View() => Buffer.Span;
    }

    public struct UInt16Scalar : INumericScalar<UInt16Type>, IDotNetStruct<ushort>
    {
        public ArrowBuffer Buffer { get; }
        public UInt16Type Type => UInt16Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public UInt16Scalar(ushort value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        internal UInt16Scalar(ArrowBuffer value)
        {
            Buffer = value;
        }

        public ushort Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        return *(ushort*)ptr;
                    }
                }
            }
        }
        public ReadOnlySpan<byte> View() => Buffer.Span;
    }
    public struct Int16Scalar : INumericScalar<Int16Type>, IDotNetStruct<short>
    {
        public ArrowBuffer Buffer { get; }
        public Int16Type Type => Int16Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public Int16Scalar(short value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        internal Int16Scalar(ArrowBuffer value)
        {
            Buffer = value;
        }

        public short Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        return *(short*)ptr;
                    }
                }
            }
        }

        public ReadOnlySpan<byte> View() => Buffer.Span;
    }

    public struct UInt32Scalar : INumericScalar<UInt32Type>, IDotNetStruct<uint>
    {
        public ArrowBuffer Buffer { get; }
        public UInt32Type Type => UInt32Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public UInt32Scalar(uint value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        internal UInt32Scalar(ArrowBuffer value)
        {
            Buffer = value;
        }

        public uint Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        return *(uint*)ptr;
                    }
                }
            }
        }

        public ReadOnlySpan<byte> View() => Buffer.Span;
    }

    public struct Int32Scalar : INumericScalar<Int32Type>, IDotNetStruct<int>
    {
        public ArrowBuffer Buffer { get; }
        public Int32Type Type => Int32Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public Int32Scalar(int value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        internal Int32Scalar(ArrowBuffer value)
        {
            Buffer = value;
        }

        public int Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        return *(int*)ptr;
                    }
                }
            }
        }

        public ReadOnlySpan<byte> View() => Buffer.Span;
    }

    public struct UInt64Scalar : INumericScalar<UInt64Type>, IDotNetStruct<ulong>
    {
        public ArrowBuffer Buffer { get; }
        public UInt64Type Type => UInt64Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public UInt64Scalar(ulong value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        internal UInt64Scalar(ArrowBuffer value)
        {
            Buffer = value;
        }

        public ulong Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        return *(ulong*)ptr;
                    }
                }
            }
        }
        public ReadOnlySpan<byte> View() => Buffer.Span;
    }
    public struct Int64Scalar : INumericScalar<Int64Type>, IDotNetStruct<long>
    {
        public ArrowBuffer Buffer { get; }
        public Int64Type Type => Int64Type.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public Int64Scalar(long value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        internal Int64Scalar(ArrowBuffer value)
        {
            Buffer = value;
        }

        public long Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        return *(long*)ptr;
                    }
                }
            }
        }
        public ReadOnlySpan<byte> View() => Buffer.Span;
    }

    public struct FloatScalar : INumericScalar<FloatType>, IDotNetStruct<float>
    {
        public ArrowBuffer Buffer { get; }
        public FloatType Type => FloatType.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public FloatScalar(float value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        internal FloatScalar(ArrowBuffer value)
        {
            Buffer = value;
        }

        public float Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        return *(float*)ptr;
                    }
                }
            }
        }
        public ReadOnlySpan<byte> View() => Buffer.Span;
    }

    public struct DoubleScalar : INumericScalar<DoubleType>, IDotNetStruct<double>
    {
        public ArrowBuffer Buffer { get; }
        public DoubleType Type => DoubleType.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public DoubleScalar(double value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        internal DoubleScalar(ArrowBuffer value)
        {
            Buffer = value;
        }

        public double Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        return *(double*)ptr;
                    }
                }
            }
        }
        public ReadOnlySpan<byte> View() => Buffer.Span;
    }

#if NET5_0_OR_GREATER
    public struct HalfFloatScalar : INumericScalar<HalfFloatType>, IDotNetStruct<Half>
    {
        public ArrowBuffer Buffer { get; }
        public HalfFloatType Type => HalfFloatType.Default;
        IArrowType IScalar.Type => Type;
        public bool IsValid => true;

        public HalfFloatScalar(Half value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        internal HalfFloatScalar(ArrowBuffer value)
        {
            Buffer = value;
        }

        public Half Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        return *(Half*)ptr;
                    }
                }
            }
        }
        public ReadOnlySpan<byte> View() => Buffer.Span;
    }
#endif

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

        public decimal Value => DecimalUtility.GetDecimal(Buffer, 0, Type.Scale, Type.ByteWidth);
        public ReadOnlySpan<byte> View() => Buffer.Span;
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

        public decimal Value => DecimalUtility.GetDecimal(Buffer, 0, Type.Scale, Type.ByteWidth);
        public ReadOnlySpan<byte> View() => Buffer.Span;
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

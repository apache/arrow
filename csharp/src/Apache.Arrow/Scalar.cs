using System;
using System.Runtime.InteropServices;
using Apache.Arrow.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    // Inspired from C++ implementation
    // https://arrow.apache.org/docs/cpp/api/scalar.html

    public struct BinaryScalar : IBaseBinaryScalar
    {
        private int _offset;
        public int ByteLength { get; }
        public ArrowBuffer Buffer { get; }

        public IArrowType Type => BinaryType.Default;

        public BinaryScalar(ArrowBuffer value)
            : this(value, value.Length, 0)
        {
        }

        public BinaryScalar(ArrowBuffer value, int length, int offset = 0)
        {
            Buffer = value;
            ByteLength = length;

            _offset = offset;
        }

        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_offset, ByteLength);
    }

    public struct StringScalar : IBaseBinaryScalar
    {
        private int _offset;
        public int ByteLength { get; }
        public ArrowBuffer Buffer { get; }

        public IArrowType Type => StringType.Default;

        public StringScalar(string value)
            : this(new ArrowBuffer(StringType.DefaultEncoding.GetBytes(value)))
        {
        }

        private StringScalar(ArrowBuffer value)
            : this(value, value.Length, 0)
        {
        }

        public StringScalar(ArrowBuffer value, int length, int offset = 0)
        {
            Buffer = value;
            ByteLength = length;

            _offset = offset;
        }

        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_offset, ByteLength);
    }

    public struct BooleanScalar : IPrimitiveScalar<BooleanType>
    {
        public bool Value { get; }

        public BooleanType Type => BooleanType.Default;

        IArrowType IScalar.Type => Type;

        public BooleanScalar(bool value)
        {
            Value = value;
        }
        public ReadOnlySpan<byte> View() => throw new NotSupportedException("Cannot get byte view from BooleanScalar");
    }

    // Numeric scalars
    public struct UInt8Scalar : INumericScalar<UInt8Type>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public UInt8Type Type => UInt8Type.Default;

        IArrowType IScalar.Type => Type;

        public UInt8Scalar(byte value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        public UInt8Scalar(ArrowBuffer value, int index = 0)
        {
            Buffer = value;
            _index = index;
        }

        public byte Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        return *(byte*)ptr;
                    }
                }
            }
        }
        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * sizeof(byte), sizeof(byte));
    }

    public struct Int8Scalar : INumericScalar<Int8Type>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public Int8Type Type => Int8Type.Default;

        IArrowType IScalar.Type => Type;

        public Int8Scalar(sbyte value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        public Int8Scalar(ArrowBuffer value, int index = 0)
        {
            Buffer = value;
            _index = index;
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
        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * sizeof(sbyte), sizeof(sbyte));
    }

    public struct UInt16Scalar : INumericScalar<UInt16Type>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public UInt16Type Type => UInt16Type.Default;

        IArrowType IScalar.Type => Type;

        public UInt16Scalar(ushort value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        public UInt16Scalar(ArrowBuffer value, int index = 0)
        {
            Buffer = value;
            _index = index;
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
        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * sizeof(ushort), sizeof(ushort));
    }
    public struct Int16Scalar : INumericScalar<Int16Type>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public Int16Type Type => Int16Type.Default;

        IArrowType IScalar.Type => Type;

        public Int16Scalar(short value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        public Int16Scalar(ArrowBuffer value, int index = 0)
        {
            Buffer = value;
            _index = index;
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

        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * sizeof(short), sizeof(short));
    }

    public struct UInt32Scalar : INumericScalar<UInt32Type>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public UInt32Type Type => UInt32Type.Default;

        IArrowType IScalar.Type => Type;

        public UInt32Scalar(uint value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        public UInt32Scalar(ArrowBuffer value, int index = 0)
        {
            Buffer = value;
            _index = index;
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

        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * sizeof(uint), sizeof(uint));
    }

    public struct Int32Scalar : INumericScalar<Int32Type>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public Int32Type Type => Int32Type.Default;

        IArrowType IScalar.Type => Type;

        public Int32Scalar(int value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        public Int32Scalar(ArrowBuffer value, int index = 0)
        {
            Buffer = value;
            _index = index;
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

        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * 4, 4);
    }

    public struct UInt64Scalar : INumericScalar<UInt64Type>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public UInt64Type Type => UInt64Type.Default;

        IArrowType IScalar.Type => Type;

        public UInt64Scalar(ulong value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        public UInt64Scalar(ArrowBuffer value, int index = 0)
        {
            Buffer = value;
            _index = index;
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
        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * sizeof(ulong), sizeof(ulong));
    }
    public struct Int64Scalar : INumericScalar<Int64Type>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public Int64Type Type => Int64Type.Default;

        IArrowType IScalar.Type => Type;

        public Int64Scalar(long value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        public Int64Scalar(ArrowBuffer value, int index = 0)
        {
            Buffer = value;
            _index = index;
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
        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * sizeof(long), sizeof(long));
    }

    public struct FloatScalar : INumericScalar<FloatType>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public FloatType Type => FloatType.Default;

        IArrowType IScalar.Type => Type;

        public FloatScalar(float value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        public FloatScalar(ArrowBuffer value, int index = 0)
        {
            Buffer = value;
            _index = index;
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
        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * sizeof(float), sizeof(float));
    }

    public struct DoubleScalar : INumericScalar<DoubleType>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public DoubleType Type => DoubleType.Default;

        IArrowType IScalar.Type => Type;

        public DoubleScalar(double value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        public DoubleScalar(ArrowBuffer value, int index = 0)
        {
            Buffer = value;
            _index = index;
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
        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * sizeof(double), sizeof(double));
    }

#if NET5_0_OR_GREATER
    public struct HalfScalar : INumericScalar<HalfFloatType>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public HalfFloatType Type => HalfFloatType.Default;

        IArrowType IScalar.Type => Type;

        public HalfScalar(float value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        public HalfScalar(ArrowBuffer value, int index = 0)
        {
            Buffer = value;
            _index = index;
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
        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * 2, 2);
    }
#endif

    public struct Decimal128Scalar : IDecimalScalar<Decimal128Type>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public Decimal128Type Type { get; }

        IArrowType IScalar.Type => Type;

        public Decimal128Scalar(decimal value)
            : this(Decimal128Type.Default, value)
        {
        }

        public Decimal128Scalar(Decimal128Type type, decimal value)
            : this(type, new ArrowBuffer(TypeReflection.AsMemoryBytes(value, type)))
        {
        }

        public Decimal128Scalar(Decimal128Type type, ArrowBuffer value, int index = 0)
        {
            Type = type;
            Buffer = value;
            _index = index;
        }

        public decimal Value => DecimalUtility.GetDecimal(Buffer, _index, Type.Scale, Type.ByteWidth);
        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * Type.ByteWidth, Type.ByteWidth);
    }

    public struct Decimal256Scalar : IDecimalScalar<Decimal256Type>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public Decimal256Type Type { get; }

        IArrowType IScalar.Type => Type;

        public Decimal256Scalar(decimal value)
            : this(Decimal256Type.SystemDefault, value)
        {
        }
        public Decimal256Scalar(Decimal256Type type, decimal value)
            : this(type, new ArrowBuffer(TypeReflection.AsMemoryBytes(value, type)))
        {
        }

        public Decimal256Scalar(Decimal256Type type, ArrowBuffer value, int index = 0)
        {
            Type = type;
            Buffer = value;
            _index = index;
        }

        public decimal Value => DecimalUtility.GetDecimal(Buffer, _index, Type.Scale, Type.ByteWidth);
        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_index * Type.ByteWidth, Type.ByteWidth);
    }

    // Nested scalars
    public struct StructScalar : IScalar
    {
        public StructType Type { get; }
        IArrowType IScalar.Type => Type;
        public IScalar[] Fields { get; }

        public bool IsValid => true;

        public StructScalar(StructType type, IScalar[] fields)
        {
            Type = type;
            Fields = fields;
        }
    }
}

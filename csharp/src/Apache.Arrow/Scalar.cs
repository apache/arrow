using System;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow.Flatbuf;
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

        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_offset, ByteLength);
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

        public string Value
        {
            get
            {
                unsafe
                {
                    fixed (byte* ptr = View())
                    {
                        int length = Encoding.UTF8.GetCharCount(ptr, ByteLength);
                        char[] buffer = new char[length];

                        fixed (char* bufferPtr = buffer)
                        {
                            Encoding.UTF8.GetChars(ptr, ByteLength, bufferPtr, length);
                        }

                        return new string(buffer);
                    }
                }
            }
        }
        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_offset, ByteLength);
    }

    public struct BooleanScalar : IPrimitiveScalar<BooleanType, bool>
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
    public struct UInt8Scalar : INumericScalar<UInt8Type, byte>
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
                        return *ptr;
                    }
                }
            }
        }
        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index, 1);
    }

    public struct Int8Scalar : INumericScalar<Int8Type, sbyte>
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
        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index, 1);
    }

    public struct UInt16Scalar : INumericScalar<UInt16Type, ushort>
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
        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index * 2, 2);
    }
    public struct Int16Scalar : INumericScalar<Int16Type, short>
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

        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index * 2, 2);
    }

    public struct UInt32Scalar : INumericScalar<UInt32Type, uint>
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

        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index * 4, 4);
    }

    public struct Int32Scalar : INumericScalar<Int32Type, int>
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

        public int Value => Buffer.GetInt(_index);

        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index * 4, 4);
    }

    public struct UInt64Scalar : INumericScalar<UInt64Type, ulong>
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
        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index * 8, 8);
    }
    public struct Int64Scalar : INumericScalar<Int64Type, long>
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
        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index * 8, 8);
    }

    public struct FloatScalar : INumericScalar<FloatType, float>
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
        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index * 4, 4);
    }

    public struct DoubleScalar : INumericScalar<DoubleType, double>
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
        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index * 8, 8);
    }

#if NET5_0_OR_GREATER
    public struct HalfFloatScalar : INumericScalar<HalfFloatType, Half>
    {
        private int _index;
        public ArrowBuffer Buffer { get; }

        public HalfFloatType Type => HalfFloatType.Default;

        IArrowType IScalar.Type => Type;

        public HalfFloatScalar(Half value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        public HalfFloatScalar(ArrowBuffer value, int index = 0)
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
        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index * 2, 2);
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
        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index * Type.ByteWidth, Type.ByteWidth);
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
        public ReadOnlySpan<byte> View() => Buffer.UnsafeView(_index * Type.ByteWidth, Type.ByteWidth);
    }

    // Nested scalars
    public struct ListScalar : IBaseListScalar
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

    public struct StructScalar : IScalar
    {
        public StructType Type { get; }
        IArrowType IScalar.Type => Type;
        public IScalar[] Fields { get; }

        public StructScalar(StructType type, IScalar[] fields)
        {
            Type = type;
            Fields = fields;
        }
    }
}

using System;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    // Inspired from C++ implementation
    // https://arrow.apache.org/docs/cpp/api/scalar.html
    public interface IScalar
    {
        IArrowType Type { get; }
    }

    public interface IDotNetScalar<T> where T : struct
    {
        T Value { get; }
    }

    public interface IPrimitiveScalarBase : IScalar
    {
        ReadOnlySpan<byte> View();
    }

    public interface IPrimitiveScalar<TArrowType> : IPrimitiveScalarBase
        where TArrowType : IArrowType
    {
        new TArrowType Type { get; }
    }

    public interface IPrimitiveScalar<TArrowType, CType> : IPrimitiveScalar<TArrowType>, IDotNetScalar<CType>
        where TArrowType : IArrowType
        where CType : struct
    {
    }

    public interface INumericScalar<TArrowType> : IPrimitiveScalar<TArrowType>
        where TArrowType : IArrowType
    {
    }

    public interface INumericScalar<TArrowType, CType> : INumericScalar<TArrowType>, IDotNetScalar<CType>
        where TArrowType : IArrowType
        where CType : struct
    {
    }

    public interface IBaseBinaryScalar : IPrimitiveScalarBase
    {
    }

    public interface IDecimalScalar<TArrowType> : INumericScalar<TArrowType, decimal>
        where TArrowType : IDecimalType
    {
    }

    // Nested Scalars
    public interface IBaseListScalar : IScalar
    {
        IArrowArray Array { get; }
        bool IsValid { get; }
    }

    public interface IStructScalar : IScalar
    {
        IScalar[] Fields { get; }
    }
}

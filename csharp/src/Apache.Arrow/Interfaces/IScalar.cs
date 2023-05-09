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

    public interface IPrimitiveScalarBase : IScalar
    {
        ReadOnlySpan<byte> View();
    }

    public interface IPrimitiveScalar<TArrowType> : IPrimitiveScalarBase
        where TArrowType : IArrowType
    {
        new TArrowType Type { get; }
    }

    public interface INumericScalar<TArrowType> : IPrimitiveScalar<TArrowType>
        where TArrowType : IArrowType
    {
    }

    public interface IBaseBinaryScalar : IPrimitiveScalarBase
    {
    }

    public interface IStructScalar : IScalar
    {
        IScalar[] Fields { get; }
    }
}

using System;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    // Inspired from C++ implementation
    // https://arrow.apache.org/docs/cpp/api/scalar.html
    public interface IScalar
    {
        IArrowType Type { get; }
        bool IsValid { get; }
    }

    public interface INullableScalar : IScalar
    {
        IScalar Value { get; }
    }

    public interface IArrowTyped<TArrowType> where TArrowType : IArrowType
    {
        TArrowType Type { get; }
    }

    public interface IDotNetStruct<T> where T : struct
    {
        T Value { get; }
    }

    public interface IPrimitiveScalarBase : IScalar
    {
        ArrowBuffer Buffer { get; }
        ReadOnlySpan<byte> View();
    }

    public interface IPrimitiveScalar<TArrowType> : IPrimitiveScalarBase, IArrowTyped<TArrowType>
        where TArrowType : IArrowType
    {
    }

    public interface INumericScalar<TArrowType> : IPrimitiveScalar<TArrowType>
        where TArrowType : IArrowType
    {
    }

    public interface IBaseBinaryScalar : IPrimitiveScalarBase
    {
    }

    public interface IBaseBinaryScalar<TArrowType> : IBaseBinaryScalar, IArrowTyped<TArrowType>
        where TArrowType : IArrowType
    {
    }

    public interface IDecimalScalar<TArrowType> : INumericScalar<TArrowType>
        where TArrowType : IDecimalType
    {
    }

    // Nested Scalars
    public interface IBaseListScalar : IScalar
    {
        IArrowArray Array { get; }
    }

    public interface IBaseListScalar<TArrowType> : IBaseListScalar, IArrowTyped<TArrowType>
        where TArrowType : IArrowType
    {
    }

    public interface IStructScalar : IScalar
    {
        IScalar[] Fields { get; }
    }

    public interface IStructScalar<TArrowType> : IStructScalar, IArrowTyped<TArrowType>
        where TArrowType : IArrowType
    {
    }
}

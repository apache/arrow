classdef UInt64Traits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.UInt64Array
        ArrayClassName = "arrow.array.UInt64Array"
        ArrayProxyClassName = "arrow.array.proxy.UInt64Array"
        TypeConstructor = @arrow.type.UInt64Type;
        TypeClassName = "arrow.type.UInt64Type"
        TypeProxyClassName = "arrow.type.proxy.UInt64Type"
        MatlabConstructor = @uint64
        MatlabClassName = "uint64"
    end

end
classdef Int64Traits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.Int64Array
        ArrayClassName = "arrow.array.Int64Array"
        ArrayProxyClassName = "arrow.array.proxy.Int64Array"
        TypeConstructor = @arrow.type.Int64Type;
        TypeClassName = "arrow.type.Int64Type"
        TypeProxyClassName = "arrow.type.proxy.Int64Type"
        MatlabConstructor = @uint64
        MatlabClassName = "uint64"
    end

end
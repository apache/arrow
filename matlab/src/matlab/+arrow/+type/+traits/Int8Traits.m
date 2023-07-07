classdef Int8Traits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.Int8Array
        ArrayClassName = "arrow.array.Int8Array"
        ArrayProxyClassName = "arrow.array.proxy.Int8Array"
        TypeConstructor = @arrow.type.Int8Type;
        TypeClassName = "arrow.type.Int8Type"
        TypeProxyClassName = "arrow.type.proxy.Int8Type"
        MatlabConstructor = @int8
        MatlabClassName = "int8"
    end

end
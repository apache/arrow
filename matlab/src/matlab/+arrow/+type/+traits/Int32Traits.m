classdef Int32Traits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.Int32Array
        ArrayClassName = "arrow.array.Int32Array"
        ArrayProxyClassName = "arrow.array.proxy.Int32Array"
        TypeConstructor = @arrow.type.Int32Type;
        TypeClassName = "arrow.type.Int32Type"
        TypeProxyClassName = "arrow.type.proxy.Int32Type"
        MatlabConstructor = @uint32
        MatlabClassName = "uint32"
    end

end
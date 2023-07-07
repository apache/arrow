classdef UInt32Traits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.UInt32Array
        ArrayClassName = "arrow.array.UInt32Array"
        ArrayProxyClassName = "arrow.array.proxy.UInt32Array"
        TypeConstructor = @arrow.type.UInt32Type;
        TypeClassName = "arrow.type.UInt32Type"
        TypeProxyClassName = "arrow.type.proxy.UInt32Type"
        MatlabConstructor = @uint32
        MatlabClassName = "uint32"
    end

end
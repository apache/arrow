classdef UInt8Traits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.UInt8Array
        ArrayClassName = "arrow.array.UInt8Array"
        ArrayProxyClassName = "arrow.array.proxy.UInt8Array"
        TypeConstructor = @arrow.type.UInt8Type;
        TypeClassName = "arrow.type.UInt8Type"
        TypeProxyClassName = "arrow.type.proxy.UInt8Type"
        MatlabConstructor = @uint8
        MatlabClassName = "uint8"
    end

end
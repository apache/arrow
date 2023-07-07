classdef UInt16Traits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.UInt16Array
        ArrayClassName = "arrow.array.UInt16Array"
        ArrayProxyClassName = "arrow.array.proxy.UInt16Array"
        TypeConstructor = @arrow.type.UInt16Type;
        TypeClassName = "arrow.type.UInt16Type"
        TypeProxyClassName = "arrow.type.proxy.UInt16Type"
        MatlabConstructor = @uint16
        MatlabClassName = "uint16"
    end

end
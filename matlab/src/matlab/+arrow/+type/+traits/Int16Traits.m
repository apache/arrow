classdef Int16Traits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.Int16Array
        ArrayClassName = "arrow.array.Int16Array"
        ArrayProxyClassName = "arrow.array.proxy.Int16Array"
        TypeConstructor = @arrow.type.Int16Type;
        TypeClassName = "arrow.type.Int16Type"
        TypeProxyClassName = "arrow.type.proxy.Int16Type"
        MatlabConstructor = @uint16
        MatlabClassName = "uint16"
    end

end
classdef Float32Traits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.Float32Array
        ArrayClassName = "arrow.array.Float32Array"
        ArrayProxyClassName = "arrow.array.proxy.Float32Array"
        TypeConstructor = @arrow.type.Float32Type;
        TypeClassName = "arrow.type.Float32Type"
        TypeProxyClassName = "arrow.type.proxy.Float32Type"
        MatlabConstructor = @single
        MatlabClassName = "single"
    end

end
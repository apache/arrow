classdef Float64Traits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.Float64Array
        ArrayClassName = "arrow.array.Float64Array"
        ArrayProxyClassName = "arrow.array.proxy.Float64Array"
        TypeConstructor = @arrow.type.Float64Type;
        TypeClassName = "arrow.type.Float64Type"
        TypeProxyClassName = "arrow.type.proxy.Float64Type"
        MatlabConstructor = @double
        MatlabClassName = "double"
    end

end
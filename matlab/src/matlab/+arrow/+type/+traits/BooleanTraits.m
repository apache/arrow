classdef BooleanTraits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.BooleanArray
        ArrayClassName = "arrow.array.BooleanArray"
        ArrayProxyClassName = "arrow.array.proxy.BooleanArray"
        TypeConstructor = @arrow.type.BooleanType;
        TypeClassName = "arrow.type.BooleanType"
        TypeProxyClassName = "arrow.type.proxy.BooleanType"
        MatlabConstructor = @logical
        MatlabClassName = "logical"
    end

end
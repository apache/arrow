classdef StringTraits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.StringArray
        ArrayClassName = "arrow.array.StringArray"
        ArrayProxyClassName = "arrow.array.proxy.StringArray"
        TypeConstructor = @arrow.type.StringType;
        TypeClassName = "arrow.type.StringType"
        TypeProxyClassName = "arrow.type.proxy.StringType"
        MatlabConstructor = @string
        MatlabClassName = "string"
    end

end
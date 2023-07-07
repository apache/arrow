classdef TimestampTraits < arrow.type.traits.Traits

    properties (Constant)
        ArrayConstructor = @arrow.array.TimestampArray
        ArrayClassName = "arrow.array.TimestampArray"
        ArrayProxyClassName = "arrow.array.proxy.TimestampArray"
        TypeConstructor = @arrow.type.TimestampType;
        TypeClassName = "arrow.type.TimestampType"
        TypeProxyClassName = "arrow.type.proxy.TimestampType"
        MatlabConstructor = @datetime
        MatlabClassName = "datetime"
    end

end
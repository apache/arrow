function typeTraits = traits(type)
    arguments
        type(1,1) arrow.type.Type
    end
    
    import arrow.type.traits.*
    typeClass = string(class(type));
    
    switch typeClass
        case "arrow.type.UInt8Type"
            typeTraits = UInt8Traits();
        case "arrow.type.UInt16Type"
            typeTraits = UInt16Traits();
        case "arrow.type.UInt32Type"
            typeTraits = UInt32Traits();
        case "arrow.type.UInt64Type"
            typeTraits = UInt64Traits();
        case "arrow.type.Int8Type"
            typeTraits = Int8Traits();
        case "arrow.type.Int16Type"
            typeTraits = Int16Traits();
        case "arrow.type.Int32Type"
            typeTraits = Int32Traits();
        case "arrow.type.Int64Type"
            typeTraits = Int64Traits();
        case "arrow.type.Float32Type"
            typeTraits = Float32Traits();
        case "arrow.type.Float64Type"
            typeTraits = Float64Traits();
        case "arrow.type.BooleanType"
            typeTraits = BooleanTraits();
        case "arrow.type.StringType"
            typeTraits = StringTraits();
        case "arrow.type.TimestampType"
            typeTraits = TimestampTraits();
        otherwise
            error("arrow:type:traits:UnknownType", "Unknown type: " + typeClass);
    end
end
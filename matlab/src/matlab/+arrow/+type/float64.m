function float64Type = float64()
%FLOAT64 Creates an arrow.type.Float64Type object
    proxy = arrow.private.proxy.create("arrow.type.proxy.Float64Type");
    float64Type = arrow.type.Float64Type(proxy);
end

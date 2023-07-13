function float32Type = float32()
%FLOAT64 Creates an arrow.type.Float32Type object
    proxy = arrow.private.proxy.create("arrow.type.proxy.Float32Type");
    float32Type = arrow.type.Float64Type(proxy);
end
function int64Type = int64()
    proxy = arrow.private.proxy.create("arrow.type.proxy.Int64Type");
    int64Type = arrow.type.Int64Type(proxy);
end


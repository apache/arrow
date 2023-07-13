function uint64Type = uint64()
    proxy = arrow.private.proxy.create("arrow.type.proxy.UInt64Type");
    uint64Type = arrow.type.UInt64Type(proxy);
end

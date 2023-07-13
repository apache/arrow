function uint32Type = uint32()
    proxy = arrow.private.proxy.create("arrow.type.proxy.UInt32Type");
    uint32Type = arrow.type.UInt32Type(proxy);
end


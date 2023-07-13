function uint8Type = uint8()
    proxy = arrow.private.proxy.create("arrow.type.proxy.UInt8Type");
    uint8Type = arrow.type.UInt8Type(proxy);
end


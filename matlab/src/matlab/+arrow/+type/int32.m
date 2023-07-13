function int32Type = int32()
    proxy = arrow.private.proxy.create("arrow.type.proxy.Int32Type");
    int32Type = arrow.type.Int32Type(proxy);
end
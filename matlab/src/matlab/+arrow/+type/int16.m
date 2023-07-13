function int16Type = int16()
%INT16 Creates an arrow.type.Int16Type object
    proxy = arrow.private.proxy.create("arrow.type.proxy.Int16Type");
    int16Type = arrow.type.Int16Type(proxy);
end

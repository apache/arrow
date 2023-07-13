function int8Type = int8()
%INT8 Creates an arrow.type.Int8Type object
    proxy = arrow.private.proxy.create("arrow.type.proxy.Int8Type");
    int8Type = arrow.type.Int8Type(proxy);
end

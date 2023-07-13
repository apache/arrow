function booleanType = boolean()
%BOOLEAN Creates an arrow.type.BooleanType object
    proxy = arrow.private.proxy.create("arrow.type.proxy.BooleanType");
    booleanType = arrow.type.BooleanType(proxy);
end


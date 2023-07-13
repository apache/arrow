function stringType = string()
%STRING Creates an arrow.type.StringType object
    proxy = arrow.private.proxy.create("arrow.type.proxy.StringType");
    stringType = arrow.type.StringType(proxy);
end


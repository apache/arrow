function timestampType = timestamp(opts)
    arguments
        opts.TimeUnit(1, 1) arrow.type.TimeUnit = arrow.type.TimeUnit.Microsecond
        opts.TimeZone(1, 1) string {mustBeNonmissing} = "" 
    end
    args = struct(TimeUnit=string(opts.TimeUnit), TimeZone=opts.TimeZone);
    proxy = arrow.private.proxy.create("arrow.type.proxy.TimestampType", args);
    timestampType = arrow.type.TimestampType(proxy);
end
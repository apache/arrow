function proxy = create(name, args)
    arguments
        name(1, 1) string {mustBeNonmissing}
    end
    arguments(Repeating)
        args
    end
    proxy = libmexclass.proxy.Proxy.create(name, args{:});
end
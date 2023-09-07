classdef NumericType < arrow.type.FixedWidthType

    methods
        function obj = NumericType(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy
            end

            obj@arrow.type.FixedWidthType(proxy);
        end
    end

    methods(Hidden)
        function data = preallocateMATLABArray(obj, length)
            traits = arrow.type.traits.traits(obj.ID);
            data = zeros([length 1], traits.MatlabClassName);
        end
    end
end
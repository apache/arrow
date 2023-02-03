classdef DoubleArray < arrow.array.internal.CustomDisplay

    properties (Access=private)
        Proxy
    end

    properties (Access=private)
        MatlabArray
    end

    methods
        function obj = DoubleArray(matlabArray)
            obj.Proxy = arrow.proxy.array.DoubleArrayProxy(matlabArray);
            obj.MatlabArray = matlabArray;
        end

        function Print(obj)
            obj.Proxy.Print();
        end
    end

end

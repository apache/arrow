classdef DoubleArray < arrow.array.internal.CustomDisplay

    properties (Access=private)
        Proxy
    end

    properties (Access=private)
        MatlabArray
    end

    methods
        function obj = DoubleArray(matlabArray)
            obj.MatlabArray = matlabArray;
            obj.Proxy = libmexclass.proxy.Proxy("Name", "arrow.proxy.array.DoubleArrayProxy", "ConstructorArguments", {obj.MatlabArray});
        end

        function Print(obj)
            obj.Proxy.Print();
        end
    end

end

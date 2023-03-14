classdef DoubleArray < matlab.mixin.CustomDisplay

    properties (Access=private)
        Proxy
    end

    properties (Access=private)
        MatlabArray
    end

    methods
        function obj = DoubleArray(matlabArray)
            obj.MatlabArray = matlabArray;
            obj.Proxy = libmexclass.proxy.Proxy("Name", "arrow.array.proxy.DoubleArray", "ConstructorArguments", {obj.MatlabArray});
        end

        function Print(obj)
            obj.Proxy.Print();
        end
    end

    methods (Access=protected)
        function displayScalarObject(obj)
            obj.Print();
        end
    end

end

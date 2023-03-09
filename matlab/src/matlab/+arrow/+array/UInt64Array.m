classdef UInt64Array

    properties (Access=private)
        Proxy
    end

    properties (Access=private)
        MatlabArray
    end

    methods
        function obj = UInt64Array(matlabArray)
            obj.MatlabArray = uint64(matlabArray);
            obj.Proxy = libmexclass.proxy.Proxy("Name", "arrow.proxy.array.UInt64ArrayProxy", "ConstructorArguments", {obj.MatlabArray});
        end

        function Print(obj)
            obj.Proxy.Print();
        end
    end

end

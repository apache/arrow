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
            obj.Proxy = arrow.proxy.array.UInt64ArrayProxy(obj.MatlabArray);
        end

        function Print(obj)
            obj.Proxy.Print();
        end
    end

end

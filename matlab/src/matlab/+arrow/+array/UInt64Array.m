classdef UInt64Array

    properties (Access=private)
        Proxy
    end

    properties (Access=private)
        MatlabArray
    end

    methods
        function obj = UInt64Array(matlabArray)
            obj.Proxy = arrow.proxy.array.UInt64ArrayProxy(uint64(matlabArray));
            obj.MatlabArray = matlabArray;
        end

        function Print(obj)
            obj.Proxy.Print();
        end
    end

end

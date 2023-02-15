classdef UInt8Array

    properties (Access=private)
        Proxy
    end

    properties (Access=private)
        MatlabArray
    end

    methods
        function obj = UInt8Array(matlabArray)
            obj.MatlabArray = uint8(matlabArray);
            obj.Proxy = arrow.proxy.array.UInt8ArrayProxy(obj.MatlabArray);
        end

        function Print(obj)
            obj.Proxy.Print();
        end
    end

end

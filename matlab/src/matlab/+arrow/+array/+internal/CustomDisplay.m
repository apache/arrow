classdef CustomDisplay < matlab.mixin.CustomDisplay
    methods (Access = protected)
        function [pg] = getPropertyGroups(obj)
            pg = [];
        end

        function displayNonScalarObject(obj)
            obj.Print();
        end
    end
end
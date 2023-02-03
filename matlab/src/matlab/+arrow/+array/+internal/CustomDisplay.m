classdef CustomDisplay < matlab.mixin.CustomDisplay
    methods (Access = protected)
        function s = getFooter(~)
            s = 'Here is my custom footer';
        end
    end
end
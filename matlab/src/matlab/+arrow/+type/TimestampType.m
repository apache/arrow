classdef TimestampType < arrow.type.PrimitiveType
    %TIMESTAMPTYPE Summary of this class goes here
    %   Detailed explanation goes here
    
    properties(SetAccess=private)
        TimeZone(1, 1) string
        TimeUnit(1, 1) arrow.type.TimeUnit
    end

    properties(SetAccess = protected)
        ID = arrow.type.ID.Timestamp
    end

    methods
        function obj = TimestampType(opts)
        %TIMESTAMPTYPE Construct an instance of this class
            arguments
                opts.TimeUnit(1, 1) arrow.type.TimeUnit = arrow.type.TimeUnit.Microsecond
                opts.TimeZone(1, 1) string {mustBeNonmissing} = "" 
            end
            obj.TimeUnit = opts.TimeUnit;
            obj.TimeZone = opts.TimeZone;
        end
    end
end


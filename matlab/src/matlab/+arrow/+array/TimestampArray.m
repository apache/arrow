classdef TimestampArray < arrow.array.Array
    %TIMESTAMPARRAY Summary of this class goes here
    %   Detailed explanation goes here
    
    properties(Access=private)
        NullSubstitutionValue = NaT;
    end

    properties(SetAccess=private, GetAccess=public)
        Type = arrow.type.TimestampType % temporarily default value
    end

    methods
        function obj = TimestampArray(data, opts)
            arguments
                data
                opts.TimeUnit(1, 1) arrow.type.TimeUnit = arrow.type.TimeUnit.Microsecond
                opts.InferNulls(1, 1) logical = true
                opts.Valid
            end
            arrow.args.validateTypeAndShape(data, "datetime");
            validElements = arrow.args.parseValidElements(data, opts);
            ptime = arrow.array.TimestampArray.convertToEpochTime(data, opts.TimeUnit);
            timezone = string(data.TimeZone);

            args = struct(MatlabArray=ptime, Valid=validElements, TimeZone=timezone, TimeUnit=string(opts.TimeUnit));
            obj@arrow.array.Array("Name", "arrow.array.proxy.TimestampArray", "ConstructorArguments", {args});
            obj.Type = arrow.type.TimestampType(TimeUnit=opts.TimeUnit, TimeZone=timezone);
        end

        function dates = toMATLAB(obj)
            time = obj.Proxy.toMATLAB();

            epoch = datetime(1970, 1, 1, TimeZone="UTC");

            tz = obj.Type.TimeZone;
            ticsPerSecond = obj.Type.TimeUnit.TicksPerSecond;
            
            dates = datetime(time, ConvertFrom="epochtime", Epoch=epoch, ...
                TimeZone=tz, TicksPerSecond=ticsPerSecond);

            dates(~obj.Valid) = obj.NullSubstitutionValue;
        end

        function dates = datetime(obj)
            dates = toMATLAB(obj);
        end
    end

    methods (Static, Access = private)
        function time = convertToEpochTime(dates, units)

            time = zeros(size(dates), "int64");
            indices = ~isnat(dates);

            % convertTo uses Jan-1-1970 as the default epoch. If the input
            % datetime array has a TimeZone, the epoch is Jan-1-1970 UTC.
            %
            % TODO: convertTo may error if the datetime is 2^63-1 before or
            % after the epoch. We should throw a custom error in this case.
            time(indices) = convertTo(dates(indices), "epochtime", TicksPerSecond=units.TicksPerSecond);
        end
    end
end
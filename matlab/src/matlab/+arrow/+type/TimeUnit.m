classdef TimeUnit

    enumeration
        Second
        Millisecond
        Microsecond
        Nanosecond
    end

    properties (Dependent)
        TicksPerSecond
    end


    methods
        function ticksPerSecond = get.TicksPerSecond(obj)
            import arrow.type.TimeUnit
            switch obj
                case TimeUnit.Second
                    ticksPerSecond = 1;
                case TimeUnit.Millisecond
                    ticksPerSecond = 1e3;
                case TimeUnit.Microsecond
                    ticksPerSecond = 1e6;
                case TimeUnit.Nanosecond
                    ticksPerSecond = 1e9;
            end
        end
    end
end
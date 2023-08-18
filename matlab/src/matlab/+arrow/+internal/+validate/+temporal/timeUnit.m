function timeUnit(temporalType, timeUnit)
%TIMEUNIT Validates whether the given arrow.type.TimeUnit enumeration
% value is supported for the specified temporal type.
    arguments
        temporalType (1,1) string {mustBeMember(temporalType, ["Time32", "Time64"])}
        timeUnit (1,1) arrow.type.TimeUnit
    end
    import arrow.type.TimeUnit

    switch temporalType
        case "Time32"
            if ~(timeUnit == TimeUnit.Second || timeUnit == TimeUnit.Millisecond)
                errid = "arrow:validate:temporal:UnsupportedTime32TimeUnit";
                msg = "Supported TimeUnit values for Time32Type are ""Second"" and ""Millisecond"".";
                error(errid, msg);
            end
        case "Time64"
            if ~(timeUnit == TimeUnit.Microsecond || timeUnit == TimeUnit.Nanosecond)
                errid = "arrow:validate:temporal:UnsupportedTime64TimeUnit";
                msg = "Supported TimeUnit values for Time64Type are ""Microsecond"" and ""Nanosecond"".";
                error(errid, msg);
            end
    end
end

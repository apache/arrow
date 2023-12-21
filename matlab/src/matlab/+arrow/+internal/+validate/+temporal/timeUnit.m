%TIMEUNIT Validates whether the given arrow.type.TimeUnit enumeration
% value is supported for the specified temporal type.

% Licensed to the Apache Software Foundation (ASF) under one or more
% contributor license agreements.  See the NOTICE file distributed with
% this work for additional information regarding copyright ownership.
% The ASF licenses this file to you under the Apache License, Version
% 2.0 (the "License"); you may not use this file except in compliance
% with the License.  You may obtain a copy of the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
% implied.  See the License for the specific language governing
% permissions and limitations under the License.
function timeUnit(temporalType, timeUnit)
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

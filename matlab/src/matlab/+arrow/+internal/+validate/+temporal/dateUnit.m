%DATEUNIT Validates whether the given arrow.type.DateUnit enumeration
% value is supported for the specified date type.

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
function dateUnit(dateType, dateUnit)
    arguments
        dateType (1,1) string {mustBeMember(dateType, ["Date32", "Date64"])}
        dateUnit (1,1) arrow.type.DateUnit
    end
    import arrow.type.DateUnit

    switch dateType
        case "Date32"
            if dateUnit ~= DateUnit.Day
                errid = "arrow:validate:temporal:UnsupportedDate32DateUnit";
                msg = "The DateUnit for Date32Type must be ""Day"".";
                error(errid, msg);
            end
        case "Date64"
            if dateUnit ~= DateUnit.Millisecond
                errid = "arrow:validate:temporal:UnsupportedDate64DateUnit";
                msg = "The DateUnit for Date64Type must be ""Millisecond"".";
                error(errid, msg);
            end
    end
end

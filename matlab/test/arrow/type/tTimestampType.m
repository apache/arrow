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

classdef tTimestampType < hFixedWidthType
% Test class for arrow.type.TimestampType

    properties
        ArrowType = arrow.timestamp
        TypeID = arrow.type.ID.Timestamp
        BitWidth = int32(64)
        ClassName = "arrow.type.TimestampType"
    end

    methods(Test)
        function TestClass(testCase)
        % Verify ArrowType is an object of the expected class type.
            name = string(class(testCase.ArrowType));
            testCase.verifyEqual(name, testCase.ClassName);
        end

        function DefaultTimeUnit(testCase)
        % Verify the default TimeUnit is Microsecond
            type = arrow.timestamp;
            actualUnit = type.TimeUnit;
            expectedUnit = arrow.type.TimeUnit.Microsecond; 
            testCase.verifyEqual(actualUnit, expectedUnit);
        end

        function DefaultTimeZone(testCase)
        % Verify the default TimeZone is ""
            type = arrow.timestamp;
            actualTimezone = type.TimeZone;
            expectedTimezone = "";
            testCase.verifyEqual(actualTimezone, expectedTimezone);
        end

        function SupplyTimeUnitEnum(testCase)
        % Supply TimeUnit as an enum value.
            import arrow.type.*
            expectedUnit = [TimeUnit.Second, TimeUnit.Millisecond ...
                            TimeUnit.Microsecond, TimeUnit.Nanosecond];

            for unit = expectedUnit
                type = arrow.timestamp(TimeUnit=unit);
                testCase.verifyEqual(type.TimeUnit, unit);
            end
        end

        function SupplyTimeUnitString(testCase)
        % Supply TimeUnit as an string value. Verify TimeUnit is set to
        % the appropriate TimeUnit enum value.
            import arrow.type.*
            unitString = ["second", "millisecond", "microsecond", "nanosecond"];
            expectedUnit = [TimeUnit.Second, TimeUnit.Millisecond ...
                            TimeUnit.Microsecond, TimeUnit.Nanosecond];
            
            for ii = 1:numel(unitString)
                type = arrow.timestamp(TimeUnit=unitString(ii));
                testCase.verifyEqual(type.TimeUnit, expectedUnit(ii));
            end
        end

        function SupplyTimeZone(testCase)
        % Supply the TimeZone. 
            type = arrow.timestamp(TimeZone="America/New_York");
            testCase.verifyEqual(type.TimeZone, "America/New_York");
        end

        function ErrorIfMissingStringTimeZone(testCase)
            fcn = @() arrow.timestamp(TimeZone=string(missing));
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonmissing");
        end

        function ErrorIfTimeZoneIsNonScalar(testCase)
            fcn = @() arrow.timestamp(TimeZone=["a", "b"]);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");

            fcn = @() arrow.timestamp(TimeZone=strings(0, 0));
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");
        end

        function ErrorIfAmbiguousTimeUnit(testCase)
            fcn = @() arrow.timestamp(TimeUnit="mi");
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function ErrorIfTimeUnitIsNonScalar(testCase)
            units = [arrow.type.TimeUnit.Second; arrow.type.TimeUnit.Millisecond];
            fcn = @() arrow.timestamp(TimeZone=units);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");

            units = ["second" "millisecond"];
            fcn = @() arrow.timestamp(TimeZone=units);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");
        end

        function Display(testCase)
        % Verify the display of TimestampType objects.
        %
        % Example:
        %
        %  TimestampType with properties:
        %
        %          ID: Timestamp
        %    TimeUnit: Second
        %    TimeZone: "America/Anchorage"
        %
            type = arrow.timestamp(TimeUnit="Second", TimeZone="America/Anchorage"); %#ok<NASGU>
            classnameLink = "<a href=""matlab:helpPopup arrow.type.TimestampType"" style=""font-weight:bold"">TimestampType</a>";
            header = "  " + classnameLink + " with properties:" + newline;
            body = strjust(pad(["ID:"; "TimeUnit:"; "TimeZone:"]));
            body = body + " " + ["Timestamp"; "Second"; """America/Anchorage"""];
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(type)');
            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end
    end
end

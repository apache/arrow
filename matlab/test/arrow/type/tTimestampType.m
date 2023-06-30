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

classdef tTimestampType < hPrimitiveType
% Test class for arrow.type.TimestampType

    properties
        ArrowType = arrow.type.TimestampType
        TypeID = arrow.type.ID.Timestamp
        BitWidth = 64;
    end

    methods(Test)
        function DefaultTimeUnit(testCase)
        % Verify the default TimeUnit is Microsecond
            type = arrow.type.TimestampType;
            actualUnit = type.TimeUnit;
            expectedUnit = arrow.type.TimeUnit.Microsecond; 
            testCase.verifyEqual(actualUnit, expectedUnit);
        end

        function DefaultTimeZone(testCase)
        % Verify the default TimeZone is ""
            type = arrow.type.TimestampType;
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
                type = TimestampType(TimeUnit=unit);
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
                type = TimestampType(TimeUnit=unitString(ii));
                testCase.verifyEqual(type.TimeUnit, expectedUnit(ii));
            end
        end

        function SupplyTimeZone(testCase)
        % Supply the TimeZone. 
            type = arrow.type.TimestampType(TimeZone="America/New_York");
            testCase.verifyEqual(type.TimeZone, "America/New_York");
        end

        function ErrorIfMissingStringTimeZone(testCase)
            fcn = @() arrow.type.TimestampType(TimeZone=string(missing));
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonmissing");
        end

        function ErrorIfTimeZoneIsNonScalar(testCase)
            fcn = @() arrow.type.TimestampType(TimeZone=["a", "b"]);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");

              fcn = @() arrow.type.TimestampType(TimeZone=strings(0, 0));
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");
        end

        function ErrorIfAmbiguousTimeUnit(testCase)
            fcn = @() arrow.type.TimestampType(TimeUnit="mi");
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function ErrorIfTimeUnitIsNonScalar(testCase)
            units = [arrow.type.TimeUnit.Second; arrow.type.TimeUnit.Millisecond];
            fcn = @() arrow.type.TimestampType(TimeZone=units);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");

            units = ["second" "millisecond"];
            fcn = @() arrow.type.TimestampType(TimeZone=units);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");
        end
    end
end
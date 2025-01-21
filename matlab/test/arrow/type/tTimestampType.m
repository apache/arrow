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

    properties(TestParameter)
        % Test against both "Zoned" (i.e. non-empty TimeZone value) 
        % and "Unzoned" (i.e. empty TimeZone value).
        TimeZone={'America/Anchorage', ''}
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
            fcn = @() arrow.timestamp(TimeUnit=units);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");

            units = ["second" "millisecond"];
            fcn = @() arrow.timestamp(TimeUnit=units);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");
        end

        function IsEqualTrue(testCase, TimeZone)
            % Verifies isequal method of arrow.type.TimestampType returns 
            % true if these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.TimestampType
            % 2. All inputs have the same size
            % 3. The TimeUnit values of elements at corresponding positions in the arrays are equal
            % 4. The TimeZone values of elements at corresponding positions in the arrays are equal

            % Scalar TimestampType arrays
            timestampType1 = arrow.timestamp(TimeUnit="Second", TimeZone=TimeZone);
            timestampType2 = arrow.timestamp(TimeUnit="Second", TimeZone=TimeZone);

            timestampType3 = arrow.timestamp(TimeUnit="Millisecond", TimeZone=TimeZone);
            time64Type4 = arrow.timestamp(TimeUnit="Millisecond", TimeZone=TimeZone);

            timestampType5 = arrow.timestamp(TimeUnit="Microsecond", TimeZone=TimeZone);
            timestampType6 = arrow.timestamp(TimeUnit="Microsecond", TimeZone=TimeZone);

            timestampType7 = arrow.timestamp(TimeUnit="Nanosecond", TimeZone=TimeZone);
            timestampType8 = arrow.timestamp(TimeUnit="Nanosecond", TimeZone=TimeZone);

            % Scalar TimestampType arrays
            testCase.verifyTrue(isequal(timestampType1, timestampType2));
            testCase.verifyTrue(isequal(timestampType3, time64Type4));
            testCase.verifyTrue(isequal(timestampType5, timestampType6));
            testCase.verifyTrue(isequal(timestampType7, timestampType8));

            % Non-scalar TimestampType arrays
            typeArray1 = [timestampType1 timestampType3 timestampType5 timestampType7];
            typeArray2 = [timestampType2 time64Type4 timestampType6 timestampType8];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verify isequal returns false when expected.

            timestampType1 = arrow.timestamp(TimeUnit="Second");
            timestampType2 = arrow.timestamp(TimeUnit="Millisecond");
            timestampType3 = arrow.timestamp(TimeUnit="Second", TimeZone="America/New_York");
            timestampType4 = arrow.timestamp(TimeUnit="Second", TimeZone="Pacific/Fiji");
            timestampType5 = arrow.timestamp(TimeUnit="Millisecond", TimeZone="America/New_York");

            % TimeUnit values differ
            testCase.verifyFalse(isequal(timestampType1, timestampType2));
            testCase.verifyFalse(isequal(timestampType4, timestampType5));

            % One TimestampType is zoned, while the other is unzoned.
            testCase.verifyFalse(isequal(timestampType1, timestampType3));

            % Both TimestampTypes are zoned, but have different values.
            testCase.verifyFalse(isequal(timestampType3, timestampType4));

            % Different dimensions
            typeArray1 = [timestampType1 timestampType2 timestampType3];
            typeArray2 = [timestampType1 timestampType2 timestampType3]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));

            % Different TimestampType values at corresponding elements
            typeArray3 = [timestampType1 timestampType3 timestampType4];
            typeArray4 = [timestampType1 timestampType2 timestampType4];
            testCase.verifyFalse(isequal(typeArray3, typeArray4));
        end
    end
end

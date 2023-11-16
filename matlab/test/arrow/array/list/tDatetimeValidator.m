%TDATETIMEVALIDATOR Unit tests for
%arrow.array.internal.list.DatetimeValidator

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

classdef tDatetimeValidator < matlab.unittest.TestCase

    methods (Test)
        function Smoke(testCase)
            import arrow.array.internal.list.DatetimeValidator
            validator = DatetimeValidator(datetime(2023, 10, 31));
            testCase.verifyInstanceOf(validator, "arrow.array.internal.list.DatetimeValidator");
        end

        function ClassNameGetter(testCase)
            % Verify the ClassName getter returns the expected scalar
            % string.
            import arrow.array.internal.list.DatetimeValidator

            validator = DatetimeValidator(datetime(2023, 10, 31));
            testCase.verifyEqual(validator.ClassName, "datetime");
        end

        function ClassNameNoSetter(testCase)
            % Verify ClassName property is not settable.
            import arrow.array.internal.list.DatetimeValidator

            validator = DatetimeValidator(datetime(2023, 10, 31));
            fcn = @() setfield(validator, "ClassName", "duration");
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function ZonedGetter(testCase)
            % Verify the Zoned getter returns the expected scalar
            % logical.

            import arrow.array.internal.list.DatetimeValidator
            validator = DatetimeValidator(datetime(2023, 10, 31));
            testCase.verifyEqual(validator.Zoned, false);

            validator = DatetimeValidator(datetime(2023, 10, 31, TimeZone="UTC"));
            testCase.verifyEqual(validator.Zoned, true);
        end

        function ZonedNoSetter(testCase)
            % Verify Zoned property is not settable.
            import arrow.array.internal.list.DatetimeValidator

            validator = DatetimeValidator(datetime(2023, 10, 31));
            fcn = @() setfield(validator, "Zoned", true);
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");

             validator = DatetimeValidator(datetime(2023, 10, 31, TimeZone="UTC"));
            fcn = @() setfield(validator, "Zoned", false);
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function ValidateElementNoThrow(testCase) %#ok<MANU>
            % Verify validateElement does not throw an exception if:
            %  1. the input element is a datetime
            %  2. its TimeZone property is '' and Zoned = false
            %  3. its TimeZone property is not empty and Zoned = true

            import arrow.array.internal.list.DatetimeValidator

            validator = DatetimeValidator(datetime(2023, 10, 31));
            validator.validateElement(datetime(2023, 11, 1));
            validator.validateElement(datetime(2023, 11, 1) + days(0:2));
            validator.validateElement(datetime(2023, 11, 1) + days(0:2)');
            validator.validateElement(datetime.empty(0, 1));

            validator = DatetimeValidator(datetime(2023, 10, 31, TimeZone="UTC"));
            validator.validateElement(datetime(2023, 11, 1,  TimeZone="UTC"));
            validator.validateElement(datetime(2023, 11, 1,  TimeZone="America/New_York") + days(0:2));
            validator.validateElement(datetime(2023, 11, 1,  TimeZone="Pacific/Fiji") + days(0:2)');
            emptyDatetime = datetime.empty(0, 1);
            emptyDatetime.TimeZone = "Asia/Dubai";
            validator.validateElement(emptyDatetime);
        end

        function ValidateElementExpectedZonedDatetimeError(testCase)
            % Verify validateElement throws an exception whose identifier
            % is "arrow:array:list:ExpectedZonedDatetime" if the input
            % datetime is unzoned, but the validator expected all 
            % datetimes to zoned.
            import arrow.array.internal.list.DatetimeValidator

            % validator will expect all elements to be zoned datetimes
            % because the input datetime is zoned.
            validator = DatetimeValidator(datetime(2023, 10, 31, TimeZone="UTC"));
            errorID = "arrow:array:list:ExpectedZonedDatetime";
            fcn = @() validator.validateElement(datetime(2023, 11, 1));
            testCase.verifyError(fcn, errorID);
        end

        function ValidateElementExpectedUnzonedDatetimeError(testCase)
            % Verify validateElement throws an exception whose identifier
            % is "arrow:array:list:ExpectedUnzonedDatetime" if the input
            % datetime has a time zone, but the validator expected all 
            % datetimes to be unzoned.
            import arrow.array.internal.list.DatetimeValidator

            % validator will expect all elements to be unzoned datetimes
            % because the input datetime is not zoned.
            validator = DatetimeValidator(datetime(2023, 10, 31));
            errorID = "arrow:array:list:ExpectedUnzonedDatetime";
            fcn = @() validator.validateElement(datetime(2023, 11, 1, TimeZone="America/New_York"));
            testCase.verifyError(fcn, errorID);
        end

        function ValidateElementClassTypeMismatchError(testCase)
            % Verify validateElement throws an exception whose identifier
            % is "arrow:array:list:ClassTypeMismatch" if the input
            % element is not a datetime.
            import arrow.array.internal.list.DatetimeValidator

            validator = DatetimeValidator(datetime(2023, 10, 31));
            errorID = "arrow:array:list:ClassTypeMismatch";
            fcn = @() validator.validateElement(1);
            testCase.verifyError(fcn, errorID);
            fcn = @() validator.validateElement("A");
            testCase.verifyError(fcn, errorID);
            fcn = @() validator.validateElement(seconds(1));
            testCase.verifyError(fcn, errorID);
        end

        function GetElementLength(testCase)
            % Verify getElementLength returns the expected length values
            % for the given input arrays.
            import arrow.array.internal.list.DatetimeValidator

            validator = DatetimeValidator(datetime(2023, 10, 31));
            length = validator.getElementLength(datetime.empty(0, 1));
            testCase.verifyEqual(length, 0);
            length = validator.getElementLength(datetime(2023, 11, 1));
            testCase.verifyEqual(length, 1);
            length = validator.getElementLength(datetime(2023, 11, 1) + days(0:2));
            testCase.verifyEqual(length, 3);
            length = validator.getElementLength(datetime(2023, 11, 1) + days([0 1; 2 3]));
            testCase.verifyEqual(length, 4);
        end

        function ReshapeCellElements(testCase)
            % Verify reshapeCellElements reshapes all elements in the input
            % cell array into column vectors.
            import arrow.array.internal.list.DatetimeValidator

            validator = DatetimeValidator(datetime(2023, 10, 31));
            date = datetime(2023, 10, 31);
            
            C = {date + days(0:2), ...
                 date + days(3:4)', ...
                 date + days([5 6; 7 8]), ...
                 datetime.empty(1, 0)};

            act = validator.reshapeCellElements(C);

            exp = {date + days(0:2)', ...
                   date + days(3:4)', ...
                   date + days([5; 7; 6; 8]), ...
                   datetime.empty(0, 1)};

            testCase.verifyEqual(act, exp);
        end

    end

end
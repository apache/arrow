%TDATETIMEVALIDATOR Unit tests for
%arrow.array.internal.list.DatetimeValditor

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

        function HasTimeZoneGetter(testCase)
            % Verify the HasTimeZone getter returns the expected scalar
            % logical.

            import arrow.array.internal.list.DatetimeValidator
            validator = DatetimeValidator(datetime(2023, 10, 31));
            testCase.verifyEqual(validator.HasTimeZone, false);

            validator = DatetimeValidator(datetime(2023, 10, 31, TimeZone="UTC"));
            testCase.verifyEqual(validator.HasTimeZone, true);
        end

        function HasTimeZoneNoSetter(testCase)
            % Verify HasTimeZone property is not settable.
            import arrow.array.internal.list.DatetimeValidator

            validator = DatetimeValidator(datetime(2023, 10, 31));
            fcn = @() setfield(validator, "HasTimeZone", true);
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");

             validator = DatetimeValidator(datetime(2023, 10, 31, TimeZone="UTC"));
            fcn = @() setfield(validator, "HasTimeZone", false);
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function validateElementNoThrow(testCase) %#ok<MANU>
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

    end

end
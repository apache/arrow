%TTIMEUNIT Unit tests for arrow.internal.validate.temporal.timeUnit.

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

classdef tTimeUnit < matlab.unittest.TestCase

    methods(Test)

        function ErrorIfUnsupportedTemporalType(testCase)
            import arrow.internal.validate.temporal.timeUnit
            import arrow.type.TimeUnit

            temporalType = "abc";
            unit = TimeUnit.Second;
            fcn = @() timeUnit(temporalType, unit);
            errid = "MATLAB:validators:mustBeMember";
            testCase.verifyError(fcn, errid);

            temporalType = 123;
            unit = TimeUnit.Second;
            fcn = @() timeUnit(temporalType, unit);
            errid = "MATLAB:validators:mustBeMember";
            testCase.verifyError(fcn, errid);

            temporalType = [1, 2, 3];
            unit = TimeUnit.Second;
            fcn = @() timeUnit(temporalType, unit);
            errid = "MATLAB:validation:IncompatibleSize";
            testCase.verifyError(fcn, errid);
        end

        function ErrorIfUnsupportedTimeUnitType(testCase)
            import arrow.internal.validate.temporal.timeUnit
            import arrow.type.TimeUnit

            temporalType = "Time32";
            unit = "abc";
            fcn = @() timeUnit(temporalType, unit);
            errid = "MATLAB:validation:UnableToConvert";
            testCase.verifyError(fcn, errid);

            temporalType = "Time32";
            unit = 123;
            fcn = @() timeUnit(temporalType, unit);
            errid = "MATLAB:validation:UnableToConvert";
            testCase.verifyError(fcn, errid);

            temporalType = "Time32";
            unit = [1, 2, 3];
            fcn = @() timeUnit(temporalType, unit);
            errid = "MATLAB:validation:IncompatibleSize";
            testCase.verifyError(fcn, errid);
        end

        function SupportedTime32TimeUnit(testCase)
            import arrow.internal.validate.temporal.timeUnit
            import arrow.type.TimeUnit

            temporalType = "Time32";

            unit = TimeUnit.Second;
            fcn = @() timeUnit(temporalType, unit);
            testCase.verifyWarningFree(fcn);

            unit = TimeUnit.Millisecond;
            fcn = @() timeUnit(temporalType, unit);
            testCase.verifyWarningFree(fcn);
        end

        function SupportedTime64TimeUnit(testCase)
            import arrow.internal.validate.temporal.timeUnit
            import arrow.type.TimeUnit

            temporalType = "Time64";

            unit = TimeUnit.Microsecond;
            fcn = @() timeUnit(temporalType, unit);
            testCase.verifyWarningFree(fcn);

            unit = TimeUnit.Nanosecond;
            fcn = @() timeUnit(temporalType, unit);
            testCase.verifyWarningFree(fcn);
        end

        function UnsupportedTime32TimeUnit(testCase)
            import arrow.internal.validate.temporal.timeUnit
            import arrow.type.TimeUnit

            temporalType = "Time32";

            unit = TimeUnit.Microsecond;
            fcn = @() timeUnit(temporalType, unit);
            errorID = "arrow:validate:temporal:UnsupportedTime32TimeUnit";
            testCase.verifyError(fcn, errorID);

            unit = TimeUnit.Nanosecond;
            fcn = @() timeUnit(temporalType, unit);
            errorID = "arrow:validate:temporal:UnsupportedTime32TimeUnit";
            testCase.verifyError(fcn, errorID);
        end

        function UnsupportedTime64TimeUnit(testCase)
            import arrow.internal.validate.temporal.timeUnit
            import arrow.type.TimeUnit

            temporalType = "Time64";

            unit = TimeUnit.Second;
            fcn = @() timeUnit(temporalType, unit);
            errorID = "arrow:validate:temporal:UnsupportedTime64TimeUnit";
            testCase.verifyError(fcn, errorID);

            unit = TimeUnit.Millisecond;
            fcn = @() timeUnit(temporalType, unit);
            errorID = "arrow:validate:temporal:UnsupportedTime64TimeUnit";
            testCase.verifyError(fcn, errorID);
        end

    end
end

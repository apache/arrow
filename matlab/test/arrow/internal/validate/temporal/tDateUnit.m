%TDATEUNIT Unit tests for arrow.internal.validate.temporal.dateUnit.

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

classdef tDateUnit < matlab.unittest.TestCase

    methods(Test)

        function ErrorIfUnsupportedDateType(testCase)
            import arrow.internal.validate.temporal.dateUnit
            import arrow.type.DateUnit

            dateType = "abc";
            unit = DateUnit.Day;
            fcn = @() dateUnit(dateType, unit);
            errid = "MATLAB:validators:mustBeMember";
            testCase.verifyError(fcn, errid);

            dateType = 123;
            unit = DateUnit.Day;
            fcn = @() dateUnit(dateType, unit);
            errid = "MATLAB:validators:mustBeMember";
            testCase.verifyError(fcn, errid);

            dateType = [1, 2, 3];
            unit = DateUnit.Day;
            fcn = @() dateUnit(dateType, unit);
            errid = "MATLAB:validation:IncompatibleSize";
            testCase.verifyError(fcn, errid);
        end

        function ErrorIfUnsupportedDateUnitType(testCase)
            import arrow.internal.validate.temporal.dateUnit
            import arrow.type.DateUnit

            dateType = "Date32";
            unit = "abc";
            fcn = @() dateUnit(dateType, unit);
            errid = "MATLAB:validation:UnableToConvert";
            testCase.verifyError(fcn, errid);

            dateType = "Date32";
            unit = 123;
            fcn = @() dateUnit(dateType, unit);
            errid = "MATLAB:validation:UnableToConvert";
            testCase.verifyError(fcn, errid);

            dateType = "Date32";
            unit = [1, 2, 3];
            fcn = @() dateUnit(dateType, unit);
            errid = "MATLAB:validation:IncompatibleSize";
            testCase.verifyError(fcn, errid);
        end

        function SupportedDate32DateUnit(testCase)
            import arrow.internal.validate.temporal.dateUnit
            import arrow.type.DateUnit

            dateType = "Date32";

            unit = DateUnit.Day;
            fcn = @() dateUnit(dateType, unit);
            testCase.verifyWarningFree(fcn);
        end

        function SupportedDate64DateUnit(testCase)
            import arrow.internal.validate.temporal.dateUnit
            import arrow.type.DateUnit

            dateType = "Date64";

            unit = DateUnit.Millisecond;
            fcn = @() dateUnit(dateType, unit);
            testCase.verifyWarningFree(fcn);
        end

        function UnsupportedDate32DateUnit(testCase)
            import arrow.internal.validate.temporal.dateUnit
            import arrow.type.DateUnit

            dateType = "Date32";

            unit = DateUnit.Millisecond;
            fcn = @() dateUnit(dateType, unit);
            errorID = "arrow:validate:temporal:UnsupportedDate32DateUnit";
            testCase.verifyError(fcn, errorID);
        end

        function UnsupportedDate64DateUnit(testCase)
            import arrow.internal.validate.temporal.dateUnit
            import arrow.type.DateUnit

            dateType = "Date64";

            unit = DateUnit.Day;
            fcn = @() dateUnit(dateType, unit);
            errorID = "arrow:validate:temporal:UnsupportedDate64DateUnit";
            testCase.verifyError(fcn, errorID);
        end

    end

end

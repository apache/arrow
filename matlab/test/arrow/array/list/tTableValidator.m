%TTABLEVALIDATOR Unit tests for arrow.array.internal.list.TableValidator

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

classdef tTableValidator < matlab.unittest.TestCase
    
    properties (Constant)
        BaseTable = table(1, "A", datetime(2023, 11, 1, TimeZone="UTC"), ...
                VariableNames=["Number", "Letter", "Date"]);
    end

    methods(Test)
        function Smoke(testCase)
            import arrow.array.internal.list.TableValidator
            validator = TableValidator(testCase.BaseTable);
            testCase.verifyInstanceOf(validator, "arrow.array.internal.list.TableValidator");
        end

        function TableWithZeroVariablesError(testCase)
            import arrow.array.internal.list.TableValidator
            fcn = @() TableValidator(table);
            testCase.verifyError(fcn, "arrow:array:list:TableWithZeroVariables");
        end

        function VariableNamesGetter(testCase)
            import arrow.array.internal.list.TableValidator
            validator = TableValidator(testCase.BaseTable);
            testCase.verifyEqual(validator.VariableNames, ["Number", "Letter", "Date"]);
        end

        function VariableNamesNoSetter(testCase)
            import arrow.array.internal.list.TableValidator
            validator = TableValidator(testCase.BaseTable);
            fcn = @() setfield(validator, "VariableNames", ["A", "B", "C"]);
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function VariableValidatorsGetter(testCase)
            import arrow.array.internal.list.TableValidator
            import arrow.array.internal.list.DatetimeValidator
            import arrow.array.internal.list.ClassTypeValidator

            validator = TableValidator(testCase.BaseTable);

            numberVariableValidator = ClassTypeValidator(1);
            letterVariableValidator = ClassTypeValidator("A");
            datetimeVariableValidator = DatetimeValidator(datetime(2023, 10, 31, TimeZone="UTC"));
            expectedValidators = [numberVariableValidator letterVariableValidator datetimeVariableValidator];
            testCase.verifyEqual(validator.VariableValidators, expectedValidators);
        end

        function VariableValidatorsNoSetter(testCase)
            import arrow.array.internal.list.TableValidator
            import arrow.array.internal.list.ClassTypeValidator

            validator = TableValidator(testCase.BaseTable);
            numberVariableValidator = ClassTypeValidator(1);
            fcn = @() setfield(validator, "VariableValidators", numberVariableValidator);
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function ClassNameGetter(testCase)
            import arrow.array.internal.list.TableValidator
            validator = TableValidator(testCase.BaseTable);
            testCase.verifyEqual(validator.ClassName, "table");
        end

        function ClassNameNoSetter(testCase)
            import arrow.array.internal.list.TableValidator
            validator = TableValidator(testCase.BaseTable);
            fcn = @() setfield(validator, "ClassName", "string");
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end
    end
end
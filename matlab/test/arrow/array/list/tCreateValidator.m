%TCREATEVALIDATOR Unit tests for arrow.array.internal.list.createValidator.

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

classdef tCreateValidator < matlab.unittest.TestCase

    properties (TestParameter)
        NumericTypes
    end

    methods (TestParameterDefinition, Static)
        function NumericTypes = initializeNumericTypes()
            NumericTypes = {"uint8", ...
                            "uint16", ...
                            "uint32", ...
                            "uint64", ...
                            "int8", ...
                            "int16", ...
                            "int32", ...
                            "int64", ...
                            "single", ...
                            "double"};
        end
    end

    methods (Test)
        function TestNumericTypes(testCase, NumericTypes)
            % Verify createValidator returns a ClassTypeValidator with the
            % expected ClassName value when given a numeric array as input.
            import arrow.array.internal.list.createValidator
            data = cast(1, NumericTypes);
            validator = createValidator(data);
            testCase.verifyInstanceOf(validator, "arrow.array.internal.list.ClassTypeValidator");
            testCase.verifyEqual(validator.ClassName, NumericTypes);
        end

        function TestLogical(testCase)
            % Verify createValidator returns a ClassTypeValidator whose
            % ClassName property is set to "logical" when given a logical
            % array as input.
            import arrow.array.internal.list.createValidator
            data = true;
            validator = createValidator(data);
            testCase.verifyInstanceOf(validator, "arrow.array.internal.list.ClassTypeValidator");
            testCase.verifyEqual(validator.ClassName, "logical");
        end

        function TestDuration(testCase)
            % Verify createValidator returns a ClassTypeValidator whose
            % ClassName property is set to "duration" when given a duration
            % array as input.
            import arrow.array.internal.list.createValidator
            data = seconds(1);
            validator = createValidator(data);
            testCase.verifyInstanceOf(validator, "arrow.array.internal.list.ClassTypeValidator");
            testCase.verifyEqual(validator.ClassName, "duration");
        end

        function TestString(testCase)
            % Verify createValidator returns a ClassTypeValidator whose
            % ClassName property is set to "string" when given a string
            % array as input.
            import arrow.array.internal.list.createValidator
            data = "Hello World";
            validator = createValidator(data);
            testCase.verifyInstanceOf(validator, "arrow.array.internal.list.ClassTypeValidator");
            testCase.verifyEqual(validator.ClassName, "string");
        end

        function TestCell(testCase)
            % Verify createValidator returns a ClassTypeValidator whose
            % ClassName property is set to "cell" when given a cell
            % array as input.
            import arrow.array.internal.list.createValidator
            data = {"Hello World"};
            validator = createValidator(data);
            testCase.verifyInstanceOf(validator, "arrow.array.internal.list.ClassTypeValidator");
            testCase.verifyEqual(validator.ClassName, "cell");
        end

        function TestDatetime(testCase)
            % Verify createValidator returns a DatetimeValidator when given
            % a datetime array as input.
            import arrow.array.internal.list.createValidator
            data = datetime(2023, 10, 31);
            validator = createValidator(data);
            testCase.verifyInstanceOf(validator, "arrow.array.internal.list.DatetimeValidator");
            testCase.verifyEqual(validator.ClassName, "datetime");
            testCase.verifyEqual(validator.Zoned, false);
        end

        function TestTable(testCase)
            % Verify createValidator returns a TableValidator when given
            % a table as input.
            import arrow.array.internal.list.createValidator
            data = table(1, "A", VariableNames=["Number", "Letter"]);
            validator = createValidator(data);
            testCase.verifyInstanceOf(validator, "arrow.array.internal.list.TableValidator");
            testCase.verifyEqual(validator.VariableNames, ["Number", "Letter"]);
            testCase.verifyEqual(numel(validator.VariableValidators), 2);
            testCase.verifyInstanceOf(validator.VariableValidators(1), "arrow.array.internal.list.ClassTypeValidator");
            testCase.verifyEqual(validator.VariableValidators(1).ClassName, "double");
            testCase.verifyInstanceOf(validator.VariableValidators(2), "arrow.array.internal.list.ClassTypeValidator");
            testCase.verifyEqual(validator.VariableValidators(2).ClassName, "string");

        end
    
        function UnsupportedDataTypeError(testCase)
            % Verify createValidator throws an exception whose identifier
            % is "arrow:array:list:UnsupportedDataType" when given an
            % unsupported datatype as input.
            import arrow.array.internal.list.createValidator
            data = calyears(1);
            fcn = @() createValidator(data);
            testCase.verifyError(fcn, "arrow:array:list:UnsupportedDataType");
        end
    end
end
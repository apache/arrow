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
            % Verify the TableValidator constructor throws an exception
            % whose identifier is "arrow:array:list:TableWithZeroVariables"
            % if provided a table with zero variables.
            import arrow.array.internal.list.TableValidator
            
            fcn = @() TableValidator(table);
            testCase.verifyError(fcn, "arrow:array:list:TableWithZeroVariables");
        end

        function VariableNamesGetter(testCase)
            % Verify the VariableNames property getter returns the
            % expected string array.
            import arrow.array.internal.list.TableValidator
            
            validator = TableValidator(testCase.BaseTable);
            testCase.verifyEqual(validator.VariableNames, ["Number", "Letter", "Date"]);
        end

        function VariableNamesNoSetter(testCase)
            % Verify the VariableNames property is not settable.
            import arrow.array.internal.list.TableValidator
            
            validator = TableValidator(testCase.BaseTable);
            fcn = @() setfield(validator, "VariableNames", ["A", "B", "C"]);
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function VariableValidatorsGetter(testCase)
            % Verify the VariableValidators getter returns the expected
            % arrow.array.internal.list.Validator array.

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
            % Verify the VariableValidators property is not settable.
            import arrow.array.internal.list.TableValidator
            import arrow.array.internal.list.ClassTypeValidator

            validator = TableValidator(testCase.BaseTable);
            numberVariableValidator = ClassTypeValidator(1);
            fcn = @() setfield(validator, "VariableValidators", numberVariableValidator);
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function ClassNameGetter(testCase)
            % Verify the ClassName getter returns the expected scalar
            % string.
            import arrow.array.internal.list.TableValidator
            
            validator = TableValidator(testCase.BaseTable);
            testCase.verifyEqual(validator.ClassName, "table");
        end

        function ClassNameNoSetter(testCase)
            % Verify the ClassName property is not settable.
            import arrow.array.internal.list.TableValidator
            
            validator = TableValidator(testCase.BaseTable);
            fcn = @() setfield(validator, "ClassName", "string");
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function ValidateElementClassTypeMismatchError(testCase)
            % Verify validateElement throws an exception whose identifier
            % is "arrow:array:list:ClassTypeMismatch" if the input is
            % not a table.
            import arrow.array.internal.list.TableValidator
            
            validator = TableValidator(testCase.BaseTable);
            errorID = "arrow:array:list:ClassTypeMismatch";
            fcn = @() validator.validateElement(1);
            testCase.verifyError(fcn, errorID);
            fcn = @() validator.validateElement(seconds(1));
            testCase.verifyError(fcn, errorID);
        end

        function ValidateElementNumVariablesMismatchError(testCase)
            % Verify validateElement throws an exception whose identifier
            % is "arrow:array:list:NumVariablesMismatch" if the input table
            % does not have the expected number of variables.
            import arrow.array.internal.list.TableValidator
            
            validator = TableValidator(testCase.BaseTable);
            errorID = "arrow:array:list:NumVariablesMismatch";

            inputTable = table(1, "A", VariableNames=["Number", "Letter"]);
            fcn = @() validator.validateElement(inputTable);
            testCase.verifyError(fcn, errorID);

            inputTable = table(1, "A", datetime(2023, 10, 30, TimeZone="UTC"), seconds(1), ...
                VariableNames=["Number", "Letter", "Date", "Time"]);
            fcn = @() validator.validateElement(inputTable);
            testCase.verifyError(fcn, errorID);
        end

        function ValidateElementVariableNamesMismatchError(testCase)
            % Verify validateElement throws an exception whose identifier
            % is "arrow:array:list:NumVariablesMismatch" if the input table
            % does not have the expected variable names of variables.
            import arrow.array.internal.list.TableValidator

            validator = TableValidator(testCase.BaseTable);
            errorID = "arrow:array:list:VariableNamesMismatch";

            inputTable = table(1, "A", datetime(2023, 10, 31, TimeZone="UTC"), ...
                VariableNames=["A", "B", "C"]);
            fcn = @() validator.validateElement(inputTable);
            testCase.verifyError(fcn, errorID);
        end

        function ValidateElementErrorFromFirstVariable(testCase)
            % Verify validateElement throws an exception if there is an
            % issue with the first variable in the input table.
            import arrow.array.internal.list.TableValidator
            
            validator = TableValidator(testCase.BaseTable);

            % validator expects the second variable to be a double
            inputTable = table(true, "A", datetime(2023, 10, 31, TimeZone="UTC"), ...
                VariableNames=["Number", "Letter", "Date"]);
            fcn = @() validator.validateElement(inputTable);
            testCase.verifyError(fcn, "arrow:array:list:ClassTypeMismatch");

            % validator expects all table variables that are not tables
            % themselves to be columnar or empty
            nonColumnar = [1 2 3 4];
            inputTable = table(nonColumnar, "B", datetime(2023, 10, 31, TimeZone="UTC"), ...
                VariableNames=["Number", "Letter", "Date"]);
            fcn = @() validator.validateElement(inputTable);
            testCase.verifyError(fcn, "arrow:array:list:NonTabularVariablesMustBeColumnar");
        end

        function ValidateElementErrorFromSecondVariable(testCase)
            % Verify validateElement throws an exception if there is an
            % issue with the second variable in the input table.
            import arrow.array.internal.list.TableValidator
            
            validator = TableValidator(testCase.BaseTable);

            % validator expects the second variable to be a string
            inputTable = table(2, seconds(1), datetime(2023, 10, 31, TimeZone="UTC"), ...
                VariableNames=["Number", "Letter", "Date"]);
            fcn = @() validator.validateElement(inputTable);
            testCase.verifyError(fcn, "arrow:array:list:ClassTypeMismatch");

            % validator expects all table variables that are not tables
            % themselves to be columnar or empty
            nonColumnar = ["A" "B"];
            inputTable = table(2, nonColumnar, datetime(2023, 10, 31, TimeZone="UTC"), ...
                VariableNames=["Number", "Letter", "Date"]);
            fcn = @() validator.validateElement(inputTable);
            testCase.verifyError(fcn, "arrow:array:list:NonTabularVariablesMustBeColumnar");
        end

        function ValidateElementErrorFromThirdVariable(testCase)
            % Verify validateElement throws an exception if there is an
            % issue with the third variable in the input table.
            import arrow.array.internal.list.TableValidator
            
            validator = TableValidator(testCase.BaseTable);

            % validator expects the third variable to be a zoned datetime
            inputTable = table(2, "B", datetime(2023, 10, 31), ...
                VariableNames=["Number", "Letter", "Date"]);
            fcn = @() validator.validateElement(inputTable);
            testCase.verifyError(fcn, "arrow:array:list:ExpectedZonedDatetime");

            % validator expects the third variable to be datetime
            inputTable = table(2, "B", uint8(1), ...
                VariableNames=["Number", "Letter", "Date"]);
            fcn = @() validator.validateElement(inputTable);
            testCase.verifyError(fcn, "arrow:array:list:ClassTypeMismatch");

            % validator expects all table variables that are not tables
            % themselves to be columnar or empty
            nonColumnar = datetime(2023, 10, 31, TimeZone="UTC") + days(0:4);
            inputTable = table(2, "B", nonColumnar, VariableNames=["Number", "Letter", "Date"]);
            fcn = @() validator.validateElement(inputTable);
            testCase.verifyError(fcn, "arrow:array:list:NonTabularVariablesMustBeColumnar");
        end

        function validateElementNoThrow(testCase)
            % Verify validateElement does not throw an exception if the
            % input provided matches the expected schema.

            import arrow.array.internal.list.TableValidator
            validator = TableValidator(testCase.BaseTable);
            
            inputTable = table(2, "B", datetime(2023, 10, 31, TimeZone="UTC"), ...
                VariableNames=["Number", "Letter", "Date"]);
             validator.validateElement(inputTable);

            inputTable = repmat(inputTable, [10 1]);
            validator.validateElement(inputTable);

            % Create a 0x3 table
            inputTable = inputTable(1:0, :);
            validator.validateElement(inputTable);
        end

        function validateElementNestedTableVariable(testCase) %#ok<MANU>
            % Verify table variables that are tables themselves do not have
            % to be columnar, i.e. can have more than one variable.
            import arrow.array.internal.list.TableValidator
            
            baseTable = table(1, seconds(1), table("A", false));
            validator = TableValidator(baseTable);

            inputTable = table([1; 2], seconds([3;4]), table(["C"; "D"], [false; false]));
            validator.validateElement(inputTable);
        end

        function GetElementLength(testCase)
            % Verify GetElementLength returns the the number of rows as the
            % length of the element.
            import arrow.array.internal.list.TableValidator
            
            validator = TableValidator(testCase.BaseTable);
            
            length = validator.getElementLength(testCase.BaseTable);
            testCase.verifyEqual(length, 1);

            length = validator.getElementLength(repmat(testCase.BaseTable, [12 1]));
            testCase.verifyEqual(length, 12);

            length = validator.getElementLength(testCase.BaseTable(1:0, :));
            testCase.verifyEqual(length, 0);
        end

        function ReshapeCellElements(testCase)
            % Verify reshapeCellElements is a no-op. It should return the
            % original cell array unchanged.
            import arrow.array.internal.list.TableValidator

            validator = TableValidator(testCase.BaseTable);
            
            COriginal = {testCase.BaseTable, repmat(testCase.BaseTable, [10 1]), testCase.BaseTable(1:0, :)};
            CActual = validator.reshapeCellElements(COriginal);
            testCase.verifyEqual(COriginal, CActual);
        end
    end
end
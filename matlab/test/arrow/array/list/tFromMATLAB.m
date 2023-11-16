%TFROMMATLAB Unit tests for arrow.array.ListArray's froMATLAB method.

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

classdef tFromMATLAB < matlab.unittest.TestCase

    methods (Test)
        function EmptyCellArrayError(testCase)
            % Verify fromMATLAB throws an error whose identifier is 
            % "MATLAB:validators:mustBeNonempty" if given an empty cell
            % array as input.
            import arrow.array.ListArray

            fcn = @() ListArray.fromMATLAB({});
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonempty");
        end

        function MustBeCellArrayError(testCase)
            % Verify fromMATLAB throws an error whose identifier is
            % "MATLAB:validation:UnableToConvert" if the input provided is
            % not a cell array.
            import arrow.array.ListArray

            fcn = @() ListArray.fromMATLAB('a');
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function AllMissingCellArrayError(testCase)
            % Verify fromMATLAB throws an error whose identifier is
            % "arrow:array:list:CellArrayAllMissing" if given a cell array
            % containing only missing values.
            import arrow.array.ListArray

            C = {missing missing missing};
            fcn = @() ListArray.fromMATLAB(C);
            testCase.verifyError(fcn, "arrow:array:list:CellArrayAllMissing");
        end

        function ListOfFloat64(testCase)
            % Verify fromMATLAB creates the expected ListArray whose
            % Values property is a Float64Array.
            import arrow.array.ListArray

            C = {[1 2 3], [4 5], missing, [6 7 8], [], [9 10]};
            actual = ListArray.fromMATLAB(C);

            values = arrow.array(1:10);
            offsets = arrow.array(int32([0 3 5 5 8 8 10]));
            expected = ListArray.fromArrays(offsets, values, Valid=[1 2 4 5 6]);

            testCase.verifyEqual(actual, expected);
        end

        function ListOfStruct(testCase)
            % Verify fromMATLAB creates the expected ListArray whose
            % Values property is a StructArray.
            import arrow.array.ListArray

            Number = (1:10)';
            Text = compose("Test%d", (1:10)');
            Date = datetime(2023, 11, 2) + days(0:9)';
            T = table(Number, Text, Date);
            C = {missing, T(1:3, :), T(4, :), T(1:0, :), T(5:10, :), missing};
            actual = ListArray.fromMATLAB(C);

            values = arrow.array(T);
            offsets = arrow.array(int32([0 0 3 4 4 10 10]));
            expected = ListArray.fromArrays(offsets, values, Valid=[2 3 4 5]);

            testCase.verifyEqual(actual, expected);
        end

        function ListOfListOfString(testCase)
            % Verify fromMATLAB creates the expected ListArray whose
            % Values property is a ListArray.
            import arrow.array.ListArray

            rowOne = {["A" "B"], ["C" "D" "E"] missing};
            rowTwo = missing;
            rowThree = {"F" ["G" "H" "I"]};
            C = {rowOne, rowTwo rowThree};
            actual = ListArray.fromMATLAB(C);

            stringValues = arrow.array(["A" "B" "C" "D" "E" "F" "G" "H" "I"]);
            innerOffsets = arrow.array(int32([0 2 5 5 6 9]));
            valuesList = ListArray.fromArrays(innerOffsets, stringValues, Valid=[1 2 4 5]);

            outerOffsets = arrow.array(int32([0 3 3 5]));
            expected = ListArray.fromArrays(outerOffsets, valuesList, Valid=[1 3]);

            testCase.verifyEqual(actual, expected);
        end

        function OnlyEmptyElement(testCase)
            % Create a ListArray containing only empty elements.
            import arrow.array.ListArray

            emptyDuration = duration.empty(0, 0);

            C = {emptyDuration, emptyDuration, emptyDuration, emptyDuration};
            actual = ListArray.fromMATLAB(C);

            values = arrow.array(duration.empty);
            offsets = arrow.array(int32([0 0 0 0 0]));
            expected = ListArray.fromArrays(offsets, values);

            testCase.verifyEqual(actual, expected);
        end

        function CellOfEmptyCell(testCase)
            % Verify fromMATLAB creates a ListArray whose Values property
            % is a StringArray when given a cell array containing just an
            % empty cell array.
            import arrow.array.ListArray

            C = {{}};
            actual = ListArray.fromMATLAB(C);

            values = arrow.array(string.empty);
            offsets = arrow.array(int32([0 0]));
            expected = ListArray.fromArrays(offsets, values);

            testCase.verifyEqual(actual, expected);
        end

        function CellOfMatrices(testCase)
            % Verify fromMATLAB can handle cell arrays that contain
            % matrices instead of just vectors - i.e. the matrices are
            % reshaped as column vectors before they are concatenated
            % together.
            import arrow.array.ListArray

            C = {[1 2 3; 4 5 6], [7 8; 9 10], 11};
            actual = ListArray.fromMATLAB(C);

            values = arrow.array([1 4 2 5 3 6 7 9 8 10 11]);
            offsets = arrow.array(int32([0 6 10 11]));
            expected = ListArray.fromArrays(offsets, values);

            testCase.verifyEqual(actual, expected);
        end

        function ClassTypeMismatchError(testCase)
            % Verify fromMATLAB throws an error whose identifier is
            % "arrow:array:list:ClassTypeMismatch" if given a cell array
            % containing arrays with different class types.
            import arrow.array.ListArray

            C = {1, [2 3 4], "A", 5};
            fcn = @() ListArray.fromMATLAB(C);
            testCase.verifyError(fcn, "arrow:array:list:ClassTypeMismatch");
        end

        function VariableNamesMismatchError(testCase)
            % Verify fromMATLAB throws an error whose identifier is
            % "arrow:array:list:VariableNamesMismatch" if given a cell 
            % array containing tables whose variable names don't match.
            import arrow.array.ListArray

            C = {table(1, "A"), table(2, "B", VariableNames=["X", "Y"])};
            fcn = @() ListArray.fromMATLAB(C);
            testCase.verifyError(fcn, "arrow:array:list:VariableNamesMismatch");
        end

        function ExpectedZonedDatetimeError(testCase)
            % Verify fromMATLAB throws an error whose identifier is
            % "arrow:array:list:ExpectedZonedDatetime" if given a cell 
            % array containing zoned and unzoned datetimes - in that order.

            import arrow.array.ListArray

            C = {datetime(2023, 11, 1, TimeZone="UTC"), datetime(2023, 11, 2)}; 
            fcn = @() ListArray.fromMATLAB(C);
            testCase.verifyError(fcn, "arrow:array:list:ExpectedZonedDatetime");
        end

        function ExpectedUnzonedDatetimeError(testCase)
            % Verify fromMATLAB throws an error whose identifier is
            % "arrow:array:list:ExpectedUnzonedDatetime" if given a cell 
            % array containing unzoned and zoned datetimes - in that order.

            import arrow.array.ListArray

            C = {datetime(2023, 11, 1), datetime(2023, 11, 2, TimeZone="UTC")}; 
            fcn = @() ListArray.fromMATLAB(C);
            testCase.verifyError(fcn, "arrow:array:list:ExpectedUnzonedDatetime");
        end



    end

end
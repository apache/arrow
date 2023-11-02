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
            % "arrow:array:list:UnsupportedCellArray" if given a cell array
            % containing only missing values.
            import arrow.array.ListArray

            C = {missing missing missing};
            fcn = @() ListArray.fromMATLAB(C);
            testCase.verifyError(fcn, "arrow:array:list:UnsupportedCellArray");
        end

        function ListOfFloat64s(testCase)
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

        function ListOfStructs(testCase)
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

        function ListOfListOfStrings(testCase)
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

        function OnlyEmptyElements(testCase)
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

        function ClassTypeMismatchError(testCase)
            % Verify fromMATLAB throws an error whose identifier is
            % "arrow:array:list:ClassTypeMismatch" if given a cell array
            % containing arrays with different class types.
            import arrow.array.ListArray

            C = {1, [2 3 4], "A", 5};
            fcn = @() ListArray.fromMATLAB(C);
            testCase.verifyError(fcn, "arrow:array:list:ClassTypeMismatch");
        end

    end

end
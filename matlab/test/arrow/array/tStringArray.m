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

classdef tStringArray < matlab.unittest.TestCase
% Test class for arrow.array.StringArray

      properties
        ArrowArrayClassName(1, 1) string = "arrow.array.StringArray"
        ArrowArrayConstructorFcn = @arrow.array.StringArray.fromMATLAB
        MatlabArrayFcn = @string
        MatlabConversionFcn = @string
        NullSubstitutionValue = string(missing)
        ArrowType = arrow.string
    end

    methods(TestClassSetup)
        function verifyOnMatlabPath(tc)
            % Verify the arrow array class is on the MATLAB Search Path.
            tc.assertTrue(~isempty(which(tc.ArrowArrayClassName)), ...
                """" + tc.ArrowArrayClassName + """must be on the MATLAB path. " + ...
                "Use ""addpath"" to add folders to the MATLAB path.");
        end
    end

    methods(Test)
        function BasicTest(tc)
            A = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(["A", "B", "C"]));
            className = string(class(A));
            tc.verifyEqual(className, tc.ArrowArrayClassName);
        end

        function ToMATLAB(tc)
            % Create array from a scalar
            A1 = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn("A"));
            data = toMATLAB(A1);
            tc.verifyEqual(data, tc.MatlabArrayFcn("A"));

            % Create array from a vector
            A2 = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(["A", "B", "C"]));
            data = toMATLAB(A2);
            tc.verifyEqual(data, tc.MatlabArrayFcn(["A", "B", "C"]'));

            % Create a StringArray from an empty 0x0 string vector
            A3 = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(string.empty(0, 0)));
            data = toMATLAB(A3);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));

            % Create a StringArray from an empty 0x1 string vector
            A4= tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(string.empty(0, 1)));
            data = toMATLAB(A4);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));

            % Create a StringArray from an empty 1x0 string vector
            A5= tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(string.empty(0, 1)));
            data = toMATLAB(A5);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));
        end

        function MatlabConversion(tc)
            % Tests the type-specific conversion method (i.e. string)

            % Create array from a scalar
            A1 = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn("A"));
            data = tc.MatlabConversionFcn(A1);
            tc.verifyEqual(data, tc.MatlabArrayFcn("A"));

            % Create array from a vector
            A2 = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(["A", "B", "C"]));
            data = tc.MatlabConversionFcn(A2);
            tc.verifyEqual(data, tc.MatlabArrayFcn(["A", "B", "C"]'));

            % Create a StringArray from an empty 0x0 string vector
            A3 = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(string.empty(0, 0)));
            data = tc.MatlabConversionFcn(A3);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));

            % Create a StringArray from an empty 0x1 string vector
            A4= tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(string.empty(0, 1)));
            data = tc.MatlabConversionFcn(A4);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));

            % Create a StringArray from an empty 1x0 string vector
            A5= tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(string.empty(0, 1)));
            data = tc.MatlabConversionFcn(A5);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));
        end

        function LogicalValidNVPair(tc)
            % Verify the expected elements are treated as null when Valid
            % is provided as a logical array
            data = tc.MatlabArrayFcn(["A", "B", "C"]');
            arrowArray = tc.ArrowArrayConstructorFcn(data, Valid=[false true true]);

            expectedData = data;
            expectedData(1) = tc.NullSubstitutionValue;
            tc.verifyEqual(tc.MatlabConversionFcn(arrowArray), expectedData);
            tc.verifyEqual(toMATLAB(arrowArray), expectedData);
            tc.verifyEqual(arrowArray.Valid, [false; true; true]);
        end

        function NumericValidNVPair(tc)
            % Verify the expected elements are treated as null when Valid
            % is provided as a array of indices
            data = tc.MatlabArrayFcn(["A", "B", "C"]');
            arrowArray = tc.ArrowArrayConstructorFcn(data, Valid=[1, 2]);

            expectedData = data;
            expectedData(3) = tc.NullSubstitutionValue;
            tc.verifyEqual(tc.MatlabConversionFcn(arrowArray), expectedData);
            tc.verifyEqual(toMATLAB(arrowArray), expectedData);
            tc.verifyEqual(arrowArray.Valid, [true; true; false]);


            % Make sure the optimization where the valid-bitmap is stored as
            % a nullptr works as expected.
            expectedData = data;
            arrowArray = tc.ArrowArrayConstructorFcn(data, Valid=[1, 2, 3]);
            tc.verifyEqual(tc.MatlabConversionFcn(arrowArray), expectedData);
            tc.verifyEqual(toMATLAB(arrowArray), expectedData);
            tc.verifyEqual(arrowArray.Valid, [true; true; true]);
        end

        function ErrorIfNonVector(tc)
            data = tc.MatlabArrayFcn(["A", "B", "A", "B", "A", "B", "A", "B", "A"]);
            data = reshape(data, 3, 1, 3);
            fcn = @() tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(data));
            tc.verifyError(fcn, "arrow:array:InvalidShape");
        end

        function AllowNDimensionalEmptyArray(tc)
            data = tc.MatlabArrayFcn(reshape(string.empty(0, 0), [1 0 0]));
            arrowArray = tc.ArrowArrayConstructorFcn(data);
            tc.verifyEqual(arrowArray.NumElements, int64(0));
            tc.verifyEqual(toMATLAB(arrowArray), string.empty(0, 1));
        end

        function TestArrowType(tc)
            % Verify the array has the expected arrow.type.Type object
            data = tc.MatlabArrayFcn(["A", "B"]);
            arrowArray = tc.ArrowArrayConstructorFcn(data);
            tc.verifyEqual(arrowArray.Type.ID, tc.ArrowType.ID);
        end

        function Unicode(tc)
            % Verify that Unicode characters are preserved during round-trip
            % conversion.
            smiley = "😀";
            tree =  "🌲";
            mango = "🥭";
            
            matlabArray = tc.MatlabArrayFcn([smiley; tree; mango]);
            arrowArray = tc.ArrowArrayConstructorFcn(matlabArray);
            matlabArrayConverted = toMATLAB(arrowArray);
            tc.verifyEqual(matlabArrayConverted, matlabArray);
        end

        function Missing(tc)
            % Verify that string(missing) values get mapped to the empty
            % string value when InferNulls=false.
            matlabArray = tc.MatlabArrayFcn(["A"; string(missing); string(missing)]);
            arrowArray = tc.ArrowArrayConstructorFcn(matlabArray, InferNulls=false);
            matlabArrayConverted = toMATLAB(arrowArray);
            tc.verifyEqual(matlabArrayConverted, ["A"; ""; ""]);
        end

        function CellStr(tc)
            % Verify that a StringArray can be constructed from
            % a cell array of character vectors (i.e. cellstr).

            % Row vector
            matlabArray = {'A', 'B', 'C'};
            arrowArray = tc.ArrowArrayConstructorFcn(matlabArray);
            matlabArrayConverted = toMATLAB(arrowArray);
            tc.verifyEqual(matlabArrayConverted, string(matlabArray'));

            % Column vector
            matlabArray = {'A'; 'B'; 'C'};
            arrowArray = tc.ArrowArrayConstructorFcn(matlabArray);
            matlabArrayConverted = toMATLAB(arrowArray);
            tc.verifyEqual(matlabArrayConverted, string(matlabArray));

            % One element cellstr
            matlabArray = {''};
            arrowArray = tc.ArrowArrayConstructorFcn(matlabArray);
            matlabArrayConverted = toMATLAB(arrowArray);
            tc.verifyEqual(matlabArrayConverted, string(matlabArray));

            % Empty cell
            matlabArray = {};
            arrowArray = tc.ArrowArrayConstructorFcn(matlabArray);
            matlabArrayConverted = toMATLAB(arrowArray);
            tc.verifyEqual(matlabArrayConverted, string.empty(0, 1));
        end

        function ErrorIfChar(tc)
            % Verify that an error is thrown when a char array
            % is passed to the StringArray constructor.

            % Row vector
            matlabArray = 'abc';
            tc.verifyError(@() tc.ArrowArrayConstructorFcn(matlabArray), "arrow:array:InvalidType");

            % Column vector
            matlabArray = ['a';'b';'c'];
            tc.verifyError(@() tc.ArrowArrayConstructorFcn(matlabArray), "arrow:array:InvalidType");

            % Empty char (0x0)
            matlabArray = '';
            tc.verifyError(@() tc.ArrowArrayConstructorFcn(matlabArray), "arrow:array:InvalidType");

            % Empty char (0x1)
            matlabArray = char.empty(0, 1);
            tc.verifyError(@() tc.ArrowArrayConstructorFcn(matlabArray), "arrow:array:InvalidType");

            % Empty char (1x0)
            matlabArray = char.empty(1, 0);
            tc.verifyError(@() tc.ArrowArrayConstructorFcn(matlabArray), "arrow:array:InvalidType");
        end

        function TestIsEqualTrue(tc)
            % Verifies arrays are considered equal if:
            %
            %  1. Their Type properties are equal
            %  2. They have the same number of elements (i.e. their NumElements properties are equal)
            %  3. They have the same validity bitmap (i.e. their Valid properties are equal)
            %  4. All corresponding valid elements have the same values
            
            data1 = tc.MatlabArrayFcn(["1" "2" "3" "4"]);
            data2 = tc.MatlabArrayFcn(["1" "2" "5" "4"]);
            array1 = tc.ArrowArrayConstructorFcn(data1, Valid=[1 2 4]);
            array2 = tc.ArrowArrayConstructorFcn(data1, Valid=[1 2 4]);
            array3 = tc.ArrowArrayConstructorFcn(data2, Valid=[1 2 4]);
            
            tc.verifyTrue(isequal(array1, array2));
            tc.verifyTrue(isequal(array1, array3));

            % Test supplying more than two arrays to isequal
            tc.verifyTrue(isequal(array1, array2, array3)); 
        end

        function TestIsEqualFalse(tc)
            % Verify isequal returns false when expected.
            data1 = tc.MatlabArrayFcn(["1" "2" "3" "4"]);
            data2 = tc.MatlabArrayFcn(["5" "2" "3" "4"]);
            data3 = tc.MatlabArrayFcn(["1" "2" "3" "4" "5"]);
            array1 = tc.ArrowArrayConstructorFcn(data1, Valid=[1 2 4]);
            array2 = tc.ArrowArrayConstructorFcn(data1, Valid=[1 4]);
            array3 = tc.ArrowArrayConstructorFcn(data2, Valid=[1 2 4]);
            array4 = arrow.array([true false true false]);
            array5 = tc.ArrowArrayConstructorFcn(data3, Valid=[1 2 4]);

            % Their validity bitmaps are not equal
            tc.verifyFalse(isequal(array1, array2));

            % Not all corresponding valid elements are equal
            tc.verifyFalse(isequal(array1, array3));

            % Their Type properties are not equal
            tc.verifyFalse(isequal(array1, array4));

            % Their NumElements properties are not equal
            tc.verifyFalse(isequal(array1, array5));

            % Comparing an arrow.array.Array to a MATLAB double
            tc.verifyFalse(isequal(array1, 1));

            % Test supplying more than two arrays to isequal
            tc.verifyFalse(isequal(array1, array1, array3, array4, array5)); 
        end
    end
end

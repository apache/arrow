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
    
classdef tBooleanArray < matlab.unittest.TestCase
% Test class for arrow.array.BooleanArray

      properties
        ArrowArrayClassName(1, 1) string = "arrow.array.BooleanArray"
        ArrowArrayConstructorFcn = @arrow.array.BooleanArray.fromMATLAB
        MatlabArrayFcn = @logical
        MatlabConversionFcn = @logical
        NullSubstitutionValue = false
        ArrowType = arrow.boolean
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
            A = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn([true false true]));
            className = string(class(A));
            tc.verifyEqual(className, tc.ArrowArrayClassName);
        end

        function ToMATLAB(tc)
            % Create array from a scalar
            A1 = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(true));
            data = toMATLAB(A1);
            tc.verifyEqual(data, tc.MatlabArrayFcn(true));

            % Create array from a vector
            A2 = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn([true false true]));
            data = toMATLAB(A2);
            tc.verifyEqual(data, tc.MatlabArrayFcn([true false true]'));

            % Create a BooleanArray from an empty 0x0 logical vector
            A3 = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(logical.empty(0, 0)));
            data = toMATLAB(A3);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));

            % Create a BooleanArray from an empty 0x1 logical vector
            A4= tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(logical.empty(0, 1)));
            data = toMATLAB(A4);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));

            % Create a BooleanArray from an empty 1x0 logical vector
            A5= tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(logical.empty(0, 1)));
            data = toMATLAB(A5);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));
        end

        function MatlabConversion(tc)
        % Tests the type-specific conversion method (i.e. logical)

            % Create array from a scalar
            A1 = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(true));
            data = tc.MatlabConversionFcn(A1);
            tc.verifyEqual(data, tc.MatlabArrayFcn(true));

            % Create array from a vector
            A2 = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn([true false true]));
            data = tc.MatlabConversionFcn(A2);
            tc.verifyEqual(data, tc.MatlabArrayFcn([true false true]'));

            % Create a BooleanArray from an empty 0x0 logical vector
            A3 = tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(logical.empty(0, 0)));
            data = tc.MatlabConversionFcn(A3);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));

            % Create a BooleanArray from an empty 0x1 logical vector
            A4= tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(logical.empty(0, 1)));
            data = tc.MatlabConversionFcn(A4);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));

            % Create a BooleanArray from an empty 1x0 logical vector
            A5= tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(logical.empty(0, 1)));
            data = tc.MatlabConversionFcn(A5);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));
        end

        function LogicalValidNVPair(tc)
            % Verify the expected elements are treated as null when Valid
            % is provided as a logical array
            data = tc.MatlabArrayFcn([true false true]');
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
            data = tc.MatlabArrayFcn([true false true]');
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
            data = tc.MatlabArrayFcn([true false true false true false true false true]);
            data = reshape(data, 3, 1, 3);
            fcn = @() tc.ArrowArrayConstructorFcn(tc.MatlabArrayFcn(data));
            tc.verifyError(fcn, "arrow:array:InvalidShape");
        end

        function ErrorIfNotLogical(tc)
            data = [1 0 1];
            tc.verifyError(@() tc.ArrowArrayConstructorFcn(data), "arrow:array:InvalidType");
        end

        function AllowNDimensionalEmptyArray(tc)
            data = tc.MatlabArrayFcn(reshape([], [1 0 0]));
            A = tc.ArrowArrayConstructorFcn(data);
            tc.verifyEqual(A.NumElements, int64(0));
            tc.verifyEqual(toMATLAB(A), tc.MatlabArrayFcn(reshape([], [0 1])));
        end

        function ErrorIfSparseArray(tc)
            data = tc.MatlabArrayFcn(sparse([true false true]));
            fcn = @() tc.ArrowArrayConstructorFcn(data);
            tc.verifyError(fcn, "arrow:array:Sparse");
        end

        function TestArrowType(tc)
        % Verify the array has the expected arrow.type.Type object
            data = tc.MatlabArrayFcn([true false]);
            arrowArray = tc.ArrowArrayConstructorFcn(data);
            tc.verifyEqual(arrowArray.Type.ID, tc.ArrowType.ID);
        end

        function TestIsEqualTrue(tc)
            % Verifies arrays are considered equal if:
            %
            %  1. Their Type properties are equal
            %  2. They have the same number of elements (i.e. their NumElements properties are equal)
            %  3. They have the same validity bitmap (i.e. their Valid properties are equal)
            %  4. All corresponding valid elements have the same values

            
            data1 = tc.MatlabArrayFcn([true false true false]);
            data2 = tc.MatlabArrayFcn([true false false false]);
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

            data1 = tc.MatlabArrayFcn([true false true false]);
            data2 = tc.MatlabArrayFcn([false false true false]);
            data3 = tc.MatlabArrayFcn([true false true false false]);
            array1 = tc.ArrowArrayConstructorFcn(data1, Valid=[1 2 4]);
            array2 = tc.ArrowArrayConstructorFcn(data1, Valid=[1 4]);
            array3 = tc.ArrowArrayConstructorFcn(data2, Valid=[1 2 4]);
            array4 = arrow.array([1 2 3 4]);
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

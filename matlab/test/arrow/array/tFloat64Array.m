classdef tFloat64Array < hNumericArray
    % Tests for arrow.array.Float64Array

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
    

    properties
        ArrowArrayClassName = "arrow.array.Float64Array"
        ArrowArrayConstructor = @arrow.array.Float64Array
        MatlabConversionFcn = @double % double method on class
        MatlabArrayFcn = @double % double function
        MaxValue = realmax("double")
        MinValue = realmin("double")
    end

    methods(Test)
        function InfValues(testCase, MakeDeepCopy)
            A1 = arrow.array.Float64Array([Inf -Inf], DeepCopy=MakeDeepCopy);
            data = double(A1);
            testCase.verifyEqual(data, [Inf -Inf]');
        end

        function ErrorIfSparse(testCase, MakeDeepCopy)
            fcn = @() arrow.array.Float64Array(sparse(ones([10 1])), DeepCopy=MakeDeepCopy);
            testCase.verifyError(fcn, "MATLAB:expectedNonsparse");
        end

        function ValidBasic(testCase, MakeDeepCopy)
            % Create a MATLAB array with one null value (i.e. one NaN).
            % Verify NaN is considered a null value by default.
            matlabArray = [1, NaN, 3]';
            arrowArray = arrow.array.Float64Array(matlabArray, DeepCopy=MakeDeepCopy);
            expectedValid = [true, false, true]';
            testCase.verifyEqual(arrowArray.Valid, expectedValid);
        end

        function InferNulls(testCase, MakeDeepCopy)
            matlabArray = [1, NaN, 3];

            % Verify NaN is treated as a null value when InferNulls=true.
            arrowArray1 = arrow.array.Float64Array(matlabArray, InferNulls=true, DeepCopy=MakeDeepCopy);
            expectedValid1 = [true false true]';
            testCase.verifyEqual(arrowArray1.Valid, expectedValid1);
            testCase.verifyEqual(toMATLAB(arrowArray1), matlabArray');

            % Verify NaN is not treated as a null value when InferNulls=false.
            arrowArray2 = arrow.array.Float64Array(matlabArray, InferNulls=false, DeepCopy=MakeDeepCopy);
            expectedValid2 = [true true true]';
            testCase.verifyEqual(arrowArray2.Valid, expectedValid2);
            testCase.verifyEqual(toMATLAB(arrowArray2), matlabArray');
        end

        function ValidNoNulls(testCase, MakeDeepCopy)
            % Create a MATLAB array with no null values (i.e. no NaNs).
            matlabArray = [1, 2, 3]';
            arrowArray = arrow.array.Float64Array(matlabArray, DeepCopy=MakeDeepCopy);
            expectedValid = [true, true, true]';
            testCase.verifyEqual(arrowArray.Valid, expectedValid);
        end

        function ValidAllNulls(testCase, MakeDeepCopy)
            % Create a MATLAB array with all null values (i.e. all NaNs).
            matlabArray = [NaN, NaN, NaN]';
            arrowArray = arrow.array.Float64Array(matlabArray, DeepCopy=MakeDeepCopy);
            expectedValid = [false, false, false]';
            testCase.verifyEqual(arrowArray.Valid, expectedValid);
        end

        function ValidEmpty(testCase, MakeDeepCopy)
            % Create an empty 0x0 MATLAB array.
            matlabArray = double.empty(0, 0);
            arrowArray = arrow.array.Float64Array(matlabArray, DeepCopy=MakeDeepCopy);
            expectedValid = logical.empty(0, 1);
            testCase.verifyEqual(arrowArray.Valid, expectedValid);

            % Create an empty 0x1 MATLAB array.
            matlabArray = double.empty(0, 1);
            arrowArray = arrow.array.Float64Array(matlabArray, DeepCopy=MakeDeepCopy);
            testCase.verifyEqual(arrowArray.Valid, expectedValid);

            % Create an empty 1x0 MATLAB array.
            matlabArray = double.empty(1, 0);
            arrowArray = arrow.array.Float64Array(matlabArray, DeepCopy=MakeDeepCopy);
            testCase.verifyEqual(arrowArray.Valid, expectedValid);
        end
    end
end

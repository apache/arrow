classdef tFloat64Array < matlab.unittest.TestCase
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
    
    properties (TestParameter)
        MakeDeepCopy = {true false}
    end

    methods(TestClassSetup)
        function verifyOnMatlabPath(testCase)
            % arrow.array.Float64Array must be on the MATLAB path.
            testCase.assertTrue(~isempty(which('arrow.array.Float64Array')), ...
                '''arrow.array.Float64Array'' must be on the MATLAB path. Use ''addpath'' to add folders to the MATLAB path.');
        end
    end
    
    methods(Test)
        function Basic(testCase, MakeDeepCopy)
            A = arrow.array.Float64Array([1, 2, 3], DeepCopy=MakeDeepCopy);
            className = string(class(A));
            testCase.verifyEqual(className, "arrow.array.Float64Array");
        end

        function ShallowCopy(testCase)
            % By default, Float64Array does not create a deep copy on
            % construction when constructed from a MATLAB array. Instead,
            % it stores a shallow copy of the array keep the memory alive.
            A = arrow.array.Float64Array([1, 2, 3]);
            testCase.verifyEqual(A.MatlabArray, [1 2 3]);
            testCase.verifyEqual(double(A), [1 2 3]');

            A = arrow.array.Float64Array([1, 2, 3], DeepCopy=false);
            testCase.verifyEqual(A.MatlabArray, [1 2 3]);
            testCase.verifyEqual(double(A), [1 2 3]');
        end

        function DeepCopy(testCase)
            % Verify Float64Array does not store shallow copy of the MATLAB
            % array if DeepCopy=true was supplied.
            A = arrow.array.Float64Array([1, 2, 3], DeepCopy=true);
            testCase.verifyEqual(A.MatlabArray, []);
            testCase.verifyEqual(double(A), [1 2 3]');
        end

        function Double(testCase, MakeDeepCopy)
            % Create a Float64Array from a scalar double
            A1 = arrow.array.Float64Array(100, DeepCopy=MakeDeepCopy);
            data = double(A1);
            testCase.verifyEqual(data, 100);

            % Create a Float64Array from a double vector 
            A2 = arrow.array.Float64Array([1 2 3], DeepCopy=MakeDeepCopy);
            data = double(A2);
            testCase.verifyEqual(data, [1 2 3]');
        
            % Create a Float64Array from an empty double vector
            A3 = arrow.array.Float64Array([], DeepCopy=MakeDeepCopy);
            data = double(A3);
            testCase.verifyEqual(data, double.empty(0, 1));
        end

        function MinValue(testCase, MakeDeepCopy)
            A1 = arrow.array.Float64Array(realmin, DeepCopy=MakeDeepCopy);
            data = double(A1);
            testCase.verifyEqual(data, realmin);
        end

        function MaxValue(testCase, MakeDeepCopy)
            A1 = arrow.array.Float64Array(realmax, DeepCopy=MakeDeepCopy);
            data = double(A1);
            testCase.verifyEqual(data, realmax);
        end

        function InfValues(testCase, MakeDeepCopy)
            A1 = arrow.array.Float64Array([Inf -Inf], DeepCopy=MakeDeepCopy);
            data = double(A1);
            testCase.verifyEqual(data, [Inf -Inf]');
        end

        function ErrorIfComplex(testCase, MakeDeepCopy)
            fcn = @() arrow.array.Float64Array([10 + 1i, 4], DeepCopy=MakeDeepCopy);
            testCase.verifyError(fcn, "MATLAB:expectedReal");
        end

        function ErrorIfSparse(testCase, MakeDeepCopy)
            fcn = @() arrow.array.Float64Array(sparse(ones([10 1])), DeepCopy=MakeDeepCopy);
            testCase.verifyError(fcn, "MATLAB:expectedNonsparse");
        end

        function Length(testCase, MakeDeepCopy)
            % Zero length array
            A = arrow.array.Float64Array([], DeepCopy=MakeDeepCopy);
            expectedLength = int64(0);
            testCase.verifyEqual(A.Length, expectedLength);

            % Scalar
            A = arrow.array.Float64Array(1, DeepCopy=MakeDeepCopy);
            expectedLength = int64(1);
            testCase.verifyEqual(A.Length, expectedLength);

            % Vector
            A = arrow.array.Float64Array(1:100, DeepCopy=MakeDeepCopy);
            expectedLength = int64(100);
            testCase.verifyEqual(A.Length, expectedLength);
         end

         function toMATLAB(testCase, MakeDeepCopy)
            A1 = arrow.array.Float64Array(100, DeepCopy=MakeDeepCopy);
            data = toMATLAB(A1);
            testCase.verifyEqual(data, 100);
        end
    end
end

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
    
    methods(TestClassSetup)
        function verifyOnMatlabPath(testCase)
            % arrow.array.Float64Array must be on the MATLAB path.
            testCase.assertTrue(~isempty(which('arrow.array.Float64Array')), ...
                '''arrow.array.Float64Array'' must be on the MATLAB path. Use ''addpath'' to add folders to the MATLAB path.');
        end
    end
    
    methods(TestMethodSetup)
        function setupTempWorkingDirectory(testCase)
            import matlab.unittest.fixtures.WorkingFolderFixture;
            testCase.applyFixture(WorkingFolderFixture);
        end
    end
    
    methods(Test)
        function Basic(testCase)
            A = arrow.array.Float64Array([1, 2, 3]);
            className = string(class(A));
            testCase.verifyEqual(className, "arrow.array.Float64Array");
        end

        function Double(testCase)
            % Create a Float64Array from a scalar double
            A1 = arrow.array.Float64Array(100);
            data = double(A1);
            testCase.verifyEqual(data, 100);

            % Create a Float64Array from a double vector 
            A2 = arrow.array.Float64Array([1 2 3]);
            data = double(A2);
            testCase.verifyEqual(data, [1 2 3]');
        
            % Create a Float64Array from an empty double vector
            A3 = arrow.array.Float64Array([]);
            data = double(A3);
            testCase.verifyEqual(data, double.empty(0, 1));
        end

        function ErrorIfComplex(testCase)
            fcn = @() arrow.array.Float64Array([10 + 1i, 4]);
            testCase.verifyError(fcn, "MATLAB:expectedReal");
        end

        function ErrorIfSparse(testCase)
            fcn = @() arrow.array.Float64Array(sparse(ones([10 1])));
            testCase.verifyError(fcn, "MATLAB:expectedNonsparse");
        end
    end
end

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
    end
end

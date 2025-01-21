%TSHAPE Unit tests for arrow.internal.validate.shape.

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

classdef tShape < matlab.unittest.TestCase
    
    methods(Test)
        function ErrorIf2DimensionalMatrix(testCase)
            data = [1 2; 4 5];
            fcn = @() arrow.internal.validate.shape(data);
            errID = "arrow:array:InvalidShape";
            testCase.verifyError(fcn, errID);
        end

        function ErrorIfNDMatrix(testCase)
            data = ones([2 2 3]);
            fcn = @() arrow.internal.validate.shape(data);
            errID = "arrow:array:InvalidShape";
            testCase.verifyError(fcn, errID);
        end

        function NoErrorIfRowVector(testCase)
            data = [1 2 4 5];
            fcn = @() arrow.internal.validate.shape(data);
            testCase.verifyWarningFree(fcn);
        end

        function NoErrorIfColumnVector(testCase)
            data = [1 2 4 5]';
            fcn = @() arrow.internal.validate.shape(data);
            testCase.verifyWarningFree(fcn);
        end
    
        function NoErrorIfEmpty(testCase)
            data = double.empty(0, 0);
            fcn = @() arrow.internal.validate.shape(data);
            testCase.verifyWarningFree(fcn);

            data = double.empty(0, 1);
            fcn = @() arrow.internal.validate.shape(data);
            testCase.verifyWarningFree(fcn);

            data = double.empty(1, 0);
            fcn = @() arrow.internal.validate.shape(data);
            testCase.verifyWarningFree(fcn);

            data = double.empty(0, 1, 0);
            fcn = @() arrow.internal.validate.shape(data);
            testCase.verifyWarningFree(fcn);

             data = double.empty(0, 0, 5);
            fcn = @() arrow.internal.validate.shape(data);
            testCase.verifyWarningFree(fcn);
        end
    end    
end
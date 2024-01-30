%TNONSPARSE Unit tests for arrow.internal.validate.nonsparse.

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
classdef tNonsparse < matlab.unittest.TestCase
    
    methods(Test)
        % Test methods
        function ErrorIfSparseDouble(testCase)
            fcn = @() arrow.internal.validate.nonsparse(sparse(ones([10 1])));
            errid = "arrow:array:Sparse";
            testCase.verifyError(fcn, errid);
        end

        function ErrorIfSparseLogical(testCase)
            fcn = @() arrow.internal.validate.nonsparse(sparse(true([10 1])));
            errid = "arrow:array:Sparse";
            testCase.verifyError(fcn, errid);
        end

        function NoErrorIfNonSparseDouble(testCase)
            fcn = @() arrow.internal.validate.nonsparse(ones([10 1]));
            testCase.verifyWarningFree(fcn); 
        end

        function NoErrorIfNonSparseLogical(testCase)
            fcn = @() arrow.internal.validate.nonsparse(true([10 1]));
            testCase.verifyWarningFree(fcn); 
        end
    end
    
end
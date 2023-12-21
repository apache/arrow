%TVALIDATEARRAYLENGTHS Unit tests for
%arrow.tabular.internal.validateArrayLengths.

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

classdef tValidateArrayLengths < matlab.unittest.TestCase
    
    methods(Test)        
        function ArraysWithEqualLength(testCase)
            % Verify validateArrayLengths() does not error if all the
            % arrays have the same length.

            import arrow.tabular.internal.validateArrayLengths

            a = arrow.array(["A", "B", "C"]);
            b = arrow.array([true, false, true]);
            c = arrow.array([1, 2, 3]);

            % cell array with one element
            fcn = @() validateArrayLengths({a});
            testCase.verifyWarningFree(fcn);

            % cell array with two elements
            fcn = @() validateArrayLengths({a, b});
            testCase.verifyWarningFree(fcn);
            
            % cell array with three elements
            fcn = @() validateArrayLengths({a, b, c});
            testCase.verifyWarningFree(fcn);
        end

        function ArraysWithUnequalLengths(testCase)
            % Verify validateArrayLengths() throws an error whose
            % identifier is "arrow:tabular:UnequalArrayLengths" if
            % all the arrays do not have the same length.
            
            import arrow.tabular.internal.validateArrayLengths

            a = arrow.array(["A", "B", "C"]);
            b = arrow.array([true, false, true, true]);
            c = arrow.array([1, 2, 3]);

            fcn = @() validateArrayLengths({a, b});
            testCase.verifyError(fcn, "arrow:tabular:UnequalArrayLengths");
            
            fcn = @() validateArrayLengths({b, a});
            testCase.verifyError(fcn, "arrow:tabular:UnequalArrayLengths");

            fcn = @() validateArrayLengths({b, a, c});
            testCase.verifyError(fcn, "arrow:tabular:UnequalArrayLengths");

            fcn = @() validateArrayLengths({a, c, b});
            testCase.verifyError(fcn, "arrow:tabular:UnequalArrayLengths");
        end
    end
end
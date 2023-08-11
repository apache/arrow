%TSTRING Unit tests for arrow.internal.validate.index.string

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

classdef tString < matlab.unittest.TestCase

    
    methods(Test)
        
        function MissingStringError(testCase)
            % Verify string() throws an error whose idenitifier is 
            % "arrow:badSubscript:NonPositive" if the index array provided
            % has mising string values.

            import arrow.internal.validate.*

            errid = "arrow:badSubscript:MissingString";

            fcn = @() index.string(string(missing));
            testCase.verifyError(fcn, errid);

            fcn = @() index.string(["A" missing "B"]);
            testCase.verifyError(fcn, errid);
        end

        function ZeroLengthTextError(testCase)
            % Verify string() throws an error whose idenitifier is 
            % "arrow:badSubscript:ZeroLengthText" if the index array 
            % provided has zero length text values.

            import arrow.internal.validate.*

            errid = "arrow:badSubscript:ZeroLengthText";

            fcn = @() index.string("");
            testCase.verifyError(fcn, errid);

            fcn = @() index.string(["A" "" "B"]);
            testCase.verifyError(fcn, errid);
        end

        function ValidStringIndices(testCase)
            % Verify string() does not throw an error if given a valid
            % string index array.

            import arrow.internal.validate.*

            fcn = @() index.string("A");
            testCase.verifyWarningFree(fcn);

            fcn = @() index.string(["A" "B"]);
            testCase.verifyWarningFree(fcn);
        end

        function AssertIfNotString(testCase)
            % Verify string() throws an assertion error if the input
            % provided is not a string.

            import arrow.internal.validate.*

            fcn = @() index.string(1);
            testCase.verifyError(fcn, "MATLAB:assertion:failed");
        end
    end
end
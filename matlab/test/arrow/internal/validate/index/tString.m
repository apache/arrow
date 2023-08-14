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
            % "arrow:badsubscript:MissingString" if the index array 
            % provided has mising string values.

            import arrow.internal.validate.*

            errid = "arrow:badsubscript:MissingString";

            fcn = @() index.string(string(missing));
            testCase.verifyError(fcn, errid);

            fcn = @() index.string(["A" missing "B"]);
            testCase.verifyError(fcn, errid);
        end

        function ZeroLengthText(testCase)
            % Verify string() does not throw an error if the index array 
            % provided has zero length text values.

            import arrow.internal.validate.*

            idx = index.string("");
            testCase.verifyEqual(idx, "");

            idx = index.string(["A" "" "B"]);
            testCase.verifyEqual(idx, ["A"; ""; "B"]);
        end

        function ValidStringIndices(testCase)
            % Verify string() returns the expected string array if given
            % a valid string, char, or cellstr as the index array.

            import arrow.internal.validate.*

            idx = index.string("A");
            testCase.verifyEqual(idx, "A");

            idx = index.string(["A", "B"]);
            testCase.verifyEqual(idx, ["A"; "B"]);

            idx = index.string('ABC');
            testCase.verifyEqual(idx, "ABC");

            idx = index.string(['ABC'; 'DEF']);
            testCase.verifyEqual(idx, "ADBECF");

            idx = index.string({'Var1'});
            testCase.verifyEqual(idx, "Var1");

            idx = index.string({'Var1', 'A'});
            testCase.verifyEqual(idx, ["Var1"; "A"]);
        end

        function ErrorIfNonString(testCase)
            % Verify string() throws an error whose idenitifer is 
            % "arrow:badsubscript:NonString" if neither a string array,
            % char array, nor cellstr array was provided as the index. 

            import arrow.internal.validate.*

            fcn = @() index.string(1);
            testCase.verifyError(fcn, "arrow:badsubscript:NonString");
        end

        function OutputShape(testCase)
            % Verify string() always returns a column vector.

            import arrow.internal.validate.*

            % Provide a 2x2 matrix
            original = ["A" "B"; "C" "D"];
            expected = ["A" "C" "B" "D"]';
            actual = index.string(original);
            testCase.verifyEqual(actual, expected);

            % Provide a 1x3 vector
            original = ["A" "B" "C"];
            expected = ["A" "B" "C"]';
            actual = index.string(original);
            testCase.verifyEqual(actual, expected);

            % Provide a 3x1 vector
            original = ["A" "B" "C"]';
            expected = ["A" "B" "C"]';
            actual = index.string(original);
            testCase.verifyEqual(actual, expected);

            % Provide a 2x2x2 N-dimensional array
            original = reshape(string(char(65:72)'), 2, 2, 2);
            expected = string(char(65:72)');
            actual = index.string(original);
            testCase.verifyEqual(actual, expected);
        end
    end
end
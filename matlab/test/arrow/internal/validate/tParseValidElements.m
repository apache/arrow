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

classdef tParseValidElements < matlab.unittest.TestCase
% Tests for arrow.args.parseValidElements
    
    methods(Test)
        % Test methods
        function InferNullsTrue(testCase)
            % Verify values for which ismissing returns true for are not
            % considered valid.
            data = [1 NaN 3 NaN 5];
            validElements = parseValidElements(data, InferNulls=true);
            expectedValidElements = [true; false; true; false; true];
            testCase.verifyEqual(validElements, expectedValidElements);
        end

        function InferNullsFalse(testCase)
            % Verify all elements are treated as Valid wen InferNulls=false is
            % provided - including values for which that ismissing returns true.
            data = [1 NaN 3 NaN 5];
            validElements = parseValidElements(data, InferNulls=false);
            expectedValidElements = logical.empty(0, 1);
            testCase.verifyEqual(validElements, expectedValidElements);
        end

        function LogicalValid(testCase)
            data = [1 2 3]; 

            % Verify an empty logical array is returned when a scalar true
            % value is supplied for Valid.
            validElements = parseValidElements(data, Valid=true);
            expectedValidElements = logical.empty(0, 1);
            testCase.verifyEqual(validElements, expectedValidElements);

            % Supply a scalar false value for Valid
            validElements = parseValidElements(data, Valid=false);
            expectedValidElements = [false; false; false];
            testCase.verifyEqual(validElements, expectedValidElements);

            % Supply a logical vector for Valid
            validElements = parseValidElements(data, Valid=[false; true; true]);
            expectedValidElements = [false; true; true];
            testCase.verifyEqual(validElements, expectedValidElements);
        end

        function NumericValid(testCase)
            data = [1 2 3]; 

            % Supply 1 valid index for Valid
            validElements = parseValidElements(data, Valid=2);
            expectedValidElements = [false; true; false];
            testCase.verifyEqual(validElements, expectedValidElements);

            % Supply a numeric vector for Valid 
            validElements = parseValidElements(data, Valid=[1 3]);
            expectedValidElements = [true; false; true];
            testCase.verifyEqual(validElements, expectedValidElements);
        end

        function ComplexValidIndicesError(testCase)
            % Verify parseValidElements errors if Valid is a numeric array
            % containing complex numbers.
            data = [1 2 3];
            fcn = @() parseValidElements(data, Valid=[1i 2]);
            testCase.verifyError(fcn, "MATLAB:expectedInteger");
        end

        function FloatingPointValidIndicesError(testCase)
        % Verify parseValidElements errors if Valid is a numeric array
        % containing floating point values.
            data = [1 2 3];
            fcn = @() parseValidElements(data, Valid=[1.1 2.1]);
            testCase.verifyError(fcn, "MATLAB:expectedInteger");
        end

        function NegativeValidIndicesError(testCase)
        % Verify parseValidElements errors if Valid is a numeric array
        % containing negative numbers.
            data = [1 2 3];
            fcn = @() parseValidElements(data, Valid=-1);
            testCase.verifyError(fcn, "MATLAB:notGreater");
        end

        function ValidIndicesNotInRangeError(testCase)
        % Verify parseValidElements errors if Valid is a numeric array
        % whose values are not between 1 and the number of  elements in 
        % the data array.
            data = [1 2 3];
            fcn = @() parseValidElements(data, Valid=0);
            testCase.verifyError(fcn, "MATLAB:notGreater");

            fcn = @() parseValidElements(data, Valid=4);
            testCase.verifyError(fcn, "MATLAB:notLessEqual");
        end

        function LogicalValidTooManyError(testCase)
        % Verify parseValidElements errors if Valid is a logical array
        % with more elements than data.
            data = [1 2 3];
            fcn = @() parseValidElements(data, Valid=[true false false true]);
            testCase.verifyError(fcn, "MATLAB:incorrectNumel");
        end

        function LogicalValidTooFewError(testCase)
        % Verify parseValidElements errors if Valid is a non-scalar logical
        % array containing less elements than the data array.
            data = [1 2 3];
            fcn = @() parseValidElements(data, Valid=[true false]);
            testCase.verifyError(fcn, "MATLAB:incorrectNumel");

            fcn = @() parseValidElements(data, Valid=logical.empty(0, 1));
            testCase.verifyError(fcn, "MATLAB:incorrectNumel");
        end

        function ValidPrecedenceOverInferNulls(testCase)
        % Verify that if both InferNulls and Valid are supplied, Valid
        % takes precedence over InferNulls.

            data = [1 NaN 3];
            validElements = parseValidElements(data, InferNulls=true, Valid=true);
            expectedValidElements = logical.empty(0, 1);
            testCase.verifyEqual(validElements, expectedValidElements);

            validElements = parseValidElements(data, InferNulls=false, Valid=[true; false; false]);
            expectedValidElements = [true; false; false];
            testCase.verifyEqual(validElements, expectedValidElements);

            validElements = parseValidElements(data, InferNulls=true, Valid=[1 2]);
            expectedValidElements = [true; true; false];
            testCase.verifyEqual(validElements, expectedValidElements);

            validElements = parseValidElements(data, InferNulls=false, Valid=1);
            expectedValidElements = [true; false; false];
            testCase.verifyEqual(validElements, expectedValidElements);
        end

        function AllElementsAreValid(testCase)
        % Verify parseValidElements returns a 0x1 logical array when all
        % elements in the array are Valid.

            expectedValidElements = logical.empty(0, 1);
            validElements = parseValidElements([1 2 3], InferNulls=true);
            testCase.verifyEqual(validElements, expectedValidElements);

            validElements = parseValidElements([1 NaN 3], InferNulls=false);
            testCase.verifyEqual(validElements, expectedValidElements);

            validElements = parseValidElements([1 NaN 3], Valid=[1 2 3]);
            testCase.verifyEqual(validElements, expectedValidElements);

            validElements = parseValidElements([1 NaN 3], Valid=true);
            testCase.verifyEqual(validElements, expectedValidElements);

            validElements = parseValidElements([1 NaN 3], Valid=[true; true; true]);
            testCase.verifyEqual(validElements, expectedValidElements);

            % Pass a logical data array to parseValidElements and
            % InferNulls=true
            validElements = parseValidElements([true false true], InferNulls=true);
            testCase.verifyEqual(validElements, expectedValidElements);

            % Pass an integer data array to parseValidElements and
            % InferNulls=true
            validElements = parseValidElements(uint8([0 1 2 3]), InferNulls=true);
            testCase.verifyEqual(validElements, expectedValidElements);
        end
    end
end

function validElements = parseValidElements(data, opts)
    arguments
        data
        opts.InferNulls = true;
        opts.Valid
    end
    validElements = arrow.internal.validate.parseValidElements(data, opts);
end
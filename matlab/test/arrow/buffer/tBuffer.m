%TBUFFER Unit tests for arrow.buffer.Buffer

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

classdef tBuffer < matlab.unittest.TestCase

    methods (Test)
        function Smoke(testCase)
            import arrow.buffer.Buffer
            
            source = uint8([1 2 3]);
            buffer = Buffer.fromMATLAB(source);
            testCase.verifyInstanceOf(buffer, "arrow.buffer.Buffer");
        end

        function fromMATLABUint8Scalar(testCase)
            % Verifies fromMATLAB returns the expected arrow.buffer.Buffer
            % instance when given a scalar uint8 value.

            import arrow.buffer.Buffer
            
            source = uint8(20);
            buffer = Buffer.fromMATLAB(source);
            testCase.verifyEqual(buffer.NumBytes, int64(1));
            values = toMATLAB(buffer);
            testCase.verifyEqual(values, source);
        end

        function fromMATLABFromEmptyUint8(testCase)
            % Verifies fromMATLAB returns the expected arrow.buffer.Buffer
            % value when given an empty uint8 array.

            import arrow.buffer.Buffer
            
            % Create buffer from 0x0 uint8 array
            source = uint8.empty(0, 0);
            buffer = Buffer.fromMATLAB(source);
            testCase.verifyEqual(buffer.NumBytes, int64(0));
            values = toMATLAB(buffer);
            testCase.verifyEqual(values, uint8.empty(0, 1));

            % Create buffer from 0x1 uint8 array
            source = uint8.empty(0, 1);
            buffer = Buffer.fromMATLAB(source);
            testCase.verifyEqual(buffer.NumBytes, int64(0));
            values = toMATLAB(buffer);
            testCase.verifyEqual(values, uint8.empty(0, 1));

            % Create buffer from 1x0 uint8 array
            source = uint8.empty(0, 1);
            buffer = Buffer.fromMATLAB(source);
            testCase.verifyEqual(buffer.NumBytes, int64(0));
            values = toMATLAB(buffer);
            testCase.verifyEqual(values, uint8.empty(0, 1));
        end

        function fromMATLABUint8Vector(testCase)
            % Verifies fromMATLAB returns the expected arrow.buffer.Buffer
            % value when given an uint8 vector.

            import arrow.buffer.Buffer
            
            % Create buffer from 1x3 uint8 array
            source = uint8([3 9 27]);
            buffer = Buffer.fromMATLAB(source);
            testCase.verifyEqual(buffer.NumBytes, int64(3));
            values = toMATLAB(buffer);
            testCase.verifyEqual(values, uint8([3 9 27]'));

            % Create buffer from 3x1 uint8 array
            source =  uint8([3 9 27]');
            buffer = Buffer.fromMATLAB(source);
            testCase.verifyEqual(buffer.NumBytes, int64(3));
            values = toMATLAB(buffer);
            testCase.verifyEqual(values, uint8([3 9 27]'));
        end

        function fromMATLABDoubleVector(testCase)
            % Verifies fromMATLAB returns the expected arrow.buffer.Buffer
            % value when given an double vector.

            import arrow.buffer.Buffer
            
            % Create buffer from 1x3 double array
            source = [3 9 27];
            buffer = Buffer.fromMATLAB(source);
            testCase.verifyEqual(buffer.NumBytes, int64(24));
            values = toMATLAB(buffer);
            expected = typecast(source', "uint8");
            testCase.verifyEqual(values, expected);
            testCase.verifyEqual(typecast(values, "double"), source');

            % Create buffer from 3x1 double array
            source =  [3 9 27]';
            buffer = Buffer.fromMATLAB(source);
            testCase.verifyEqual(buffer.NumBytes, int64(24));
            values = toMATLAB(buffer);
            expected = typecast(source, "uint8");
            testCase.verifyEqual(values, expected);
            testCase.verifyEqual(typecast(values, "double"), source);
        end

        function fromMATLABNonNumericError(testCase)
            % Verify fromMATLAB throws an error if given a non-numeric
            % array as input.
            import arrow.buffer.Buffer

            fcn = @() Buffer.fromMATLAB(true);
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNumeric");

            fcn = @() Buffer.fromMATLAB("A");
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNumeric");

            fcn = @() Buffer.fromMATLAB(datetime(2023, 1, 1));
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNumeric");
        end

        function NumBytesNoSetter(testCase)
            % Verifies the NumBytes property is not settable.

            import arrow.buffer.Buffer

            buffer = Buffer.fromMATLAB([1 2 3]);
            fcn = @() setfield(buffer, "NumBytes", int64(1));
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end
    end
end
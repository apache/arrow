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

        function fromMATLABUInt8Scalar(testCase)
            % Verifies fromMATLAB returns the expected arrow.buffer.Buffer
            % instance when given a scalar uint8 value.

            import arrow.buffer.Buffer
            
            source = uint8(20);
            buffer = Buffer.fromMATLAB(source);
            testCase.verifyEqual(buffer.NumBytes, int64(1));
            values = toMATLAB(buffer);
            testCase.verifyEqual(values, source);
        end

        function fromMATLABFromEmptyUInt8(testCase)
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
            source = uint8.empty(1, 0);
            buffer = Buffer.fromMATLAB(source);
            testCase.verifyEqual(buffer.NumBytes, int64(0));
            values = toMATLAB(buffer);
            testCase.verifyEqual(values, uint8.empty(0, 1));
        end

        function fromMATLABUInt8Vector(testCase)
            % Verifies fromMATLAB returns the expected arrow.buffer.Buffer
            % value when given a uint8 vector.

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
            % value when given a double vector.

            import arrow.buffer.Buffer
            
            % Create buffer from 1x3 double array
            source = [3 9 27];
            buffer = Buffer.fromMATLAB(source);
            % Since the input vector is a 1x3 double (i.e. 64-bit floating 
            % point value)  array - each of the 3 elements takes up 8 bytes
            % (3 elements * 8 bytes per element = 24 bytes).
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

        function fromMATLABNonRealError(testCase)
            % Verify fromMATLAB throws an error if given a complex
            % numeric  array as input.
            import arrow.buffer.Buffer

            fcn = @() Buffer.fromMATLAB(10 + 3i);
            testCase.verifyError(fcn, "MATLAB:validators:mustBeReal");
        end

        function fromMATLABNonSparseError(testCase)
            % Verify fromMATLAB throws an error if given a sparse
            % numeric array as input.
            import arrow.buffer.Buffer

            fcn = @() Buffer.fromMATLAB(sparse(ones([5 1])));
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonsparse");
        end

        function NumBytesNoSetter(testCase)
            % Verifies the NumBytes property is not settable.

            import arrow.buffer.Buffer

            buffer = Buffer.fromMATLAB([1 2 3]);
            fcn = @() setfield(buffer, "NumBytes", int64(1));
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function IsEqualTrue(testCase)
            % Verifies two buffers are considered equal if
            %   1. They have the same size (NumBytes)
            %   2. They contain the same bytes

            import arrow.buffer.Buffer

            % Compare two non-empty buffers
            buffer1 = Buffer.fromMATLAB([1 2 3]);
            buffer2 = Buffer.fromMATLAB([1 2 3]);
            testCase.verifyTrue(isequal(buffer1, buffer2));

            % Compare two buffers, one of which was created from a double 
            % array and the other created from a uint8 array. However, both
            % arrays have the same bit pattern.
            data = zeros([1 24], "uint8");
            data([7 8 16 23 24]) = [240 63 64  8 64];
            buffer3 = Buffer.fromMATLAB(data);
            testCase.verifyTrue(isequal(buffer1, buffer3));

            % Compare two empty buffers 
            buffer4 = Buffer.fromMATLAB([]);
            buffer5 = Buffer.fromMATLAB([]);
            testCase.verifyTrue(isequal(buffer4, buffer5));

            % Compare more than two buffers
            testCase.verifyTrue(isequal(buffer1, buffer2, buffer3));
        end

        function IsEqualFalse(testCase)
            % Verifies two buffers are not considered equal if
            %   1. They do not have the same size (NumBytes) OR
            %   2. They do not contain the same bytes

            import arrow.buffer.Buffer
                
            buffer1 = Buffer.fromMATLAB([1 2 3]);
            buffer2 = Buffer.fromMATLAB([1 3 2]);
            buffer3 = Buffer.fromMATLAB([1 2 3 4]);
            testCase.verifyFalse(isequal(buffer1, buffer2));
            testCase.verifyFalse(isequal(buffer1, buffer3));
            testCase.verifyFalse(isequal(buffer1, buffer2, buffer3));

            % Compare a buffer to a string
            testCase.verifyFalse(isequal(buffer1, "A"));
        end
    end
end
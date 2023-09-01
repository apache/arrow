%TCHUNKEDARRAY Unit tests arrow.array.ChunkedArray

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

classdef tChunkedArray < matlab.unittest.TestCase

    properties (Constant)
        Float64Array1 = arrow.array([1 2 3 4]);
        Float64Array2 = arrow.array([NaN 6 7]);
        Float64Array3 = arrow.array([8 NaN 10 11 12]);
        Float64Type = arrow.float64()
    end

    methods (Test)
        function FromArraysTooFewInputsError(testCase)
            % Verify an error is thrown when zero input arguments are
            % provided to ChunkedArray's static method fromArrays.
            import arrow.array.ChunkedArray
            
            fcn = @() ChunkedArray.fromArrays();
            testCase.verifyError(fcn, "MATLAB:narginchk:notEnoughInputs");
        end

        function InconsistentArrayTypeError(testCase)
            % Verify an error is thrown when arrays of different types are
            % provided to ChunkedArray's static method fromArrays.
            import arrow.array.ChunkedArray
            
            float32Array = arrow.array(single([1 2 3]));
            fcn = @() ChunkedArray.fromArrays(testCase.Float64Array1, float32Array);
            testCase.verifyError(fcn, "arrow:chunkedarray:MakeFailed");
        end

        function OneChunk(testCase)
            % Verify fromArrays returns the expected ChunkedArray when one
            % array is provided as input.
            import arrow.array.ChunkedArray
            
            chunkedArray = ChunkedArray.fromArrays(testCase.Float64Array1);
            testCase.verifyChunkedArray(chunkedArray, ...
                                        NumChunks=1, ...
                                        Type=testCase.Float64Type, ...
                                        Arrays={testCase.Float64Array1});
        end

        function MultipleChunks(testCase)
            % Verify fromArrays returns the expected ChunkedArray when
            % multiple arrays are provided as input.
            import arrow.array.ChunkedArray

            arrays = {testCase.Float64Array1, testCase.Float64Array2, testCase.Float64Array3};

            chunkedArray = ChunkedArray.fromArrays(arrays{:});
            testCase.verifyChunkedArray(chunkedArray, ...
                                        NumChunks=3, ...
                                        Type=testCase.Float64Type, ...
                                        Arrays=arrays);
        end

        function TestIsEqualTrue(testCase)
            % Verifies chunked arrays are considered equal if:
            %
            %  1. Their Type properties are equal
            %  2. Their Length properties ar eequal
            %  3. The same elements are considered null
            %  4. All corresponding valid elements have the same values
            %
            % NOTE: Having the same "chunking" is not a requirement for two
            % chunked arrays to be equal. As long as "flattening" both
            % chunked arrays results in the same array, then those
            % chunked arrays are equal.

            import arrow.array.ChunkedArray

            arrays = {testCase.Float64Array1, testCase.Float64Array2, testCase.Float64Array3};
            chunkedArray1 = ChunkedArray.fromArrays(arrays{:});

            data = [toMATLAB(arrays{1}); toMATLAB(arrays{2}); toMATLAB(arrays{3})];
            floatArray = arrow.array(data);
            chunkedArray2 = ChunkedArray.fromArrays(floatArray);

            % Verify a chunked array is considered equal with itself
            testCase.verifyTrue(isequal(chunkedArray1, chunkedArray1));

            % Verify two chunked arrays are considered equal even if the
            % data is distributed differently across chunks
            testCase.verifyTrue(isequal(chunkedArray1, chunkedArray2));
        end

        function TestIsEqualFalse(testCase)
            % Verify isequal returns false when expected.
            import arrow.array.ChunkedArray

            arrays = {testCase.Float64Array1, testCase.Float64Array2, testCase.Float64Array3};
            chunkedArray1 = ChunkedArray.fromArrays(arrays{:});

            float64Array1 = arrow.array(toMATLAB(arrays{1}), InferNulls=false);
            float64Array2 = arrow.array(toMATLAB(arrays{2}), InferNulls=false);
            float64Array3 = arrow.array(toMATLAB(arrays{3}), InferNulls=false);

            chunkedArray2 = ChunkedArray.fromArrays(float64Array1, float64Array2, float64Array3);

            % Verify chunked arrays with different null values are not
            % considered equal.
            testCase.verifyFalse(isequal(chunkedArray1, chunkedArray2));

            % Verify NaNs are not treated as equal
            testCase.verifyFalse(isequal(chunkedArray2, chunkedArray2));

            % Verify chunked arrays with different lengths are not
            % considered equal
            chunkedArray3 = ChunkedArray.fromArrays(testCase.Float64Array1);
            testCase.verifyFalse(isequal(chunkedArray1, chunkedArray3));

            % Verify chunked arrays with different types are not considered
            % equal.
            
            float32Array1 = arrow.array(single(toMATLAB(arrays{1})));
            float32Array2 = arrow.array(single(toMATLAB(arrays{2})));
            float32Array3 = arrow.array(single(toMATLAB(arrays{3})));
            chunkedArray3 = ChunkedArray.fromArrays(float32Array1, float32Array2, float32Array3);
            testCase.verifyFalse(isequal(chunkedArray1, chunkedArray3));
        end

        function NumChunksNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the NumChunks property.
            import arrow.array.ChunkedArray

            arrays = {testCase.Float64Array1, testCase.Float64Array2, testCase.Float64Array3};
            chunkedArray = ChunkedArray.fromArrays(arrays{:});
            
            fcn = @() setfield(chunkedArray, "NumChunks", int32(6));
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function TypeNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the Type property.
            import arrow.array.ChunkedArray

            arrays = {testCase.Float64Array1, testCase.Float64Array2, testCase.Float64Array3};
            chunkedArray = ChunkedArray.fromArrays(arrays{:});
            
            fcn = @() setfield(chunkedArray, "Type", arrow.int32());
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function LengthNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the Type property.
            import arrow.array.ChunkedArray

            arrays = {testCase.Float64Array1, testCase.Float64Array2, testCase.Float64Array3};
            chunkedArray = ChunkedArray.fromArrays(arrays{:});
            
            fcn = @() setfield(chunkedArray, "Length", int64(100));
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

    end

    methods
        function verifyChunkedArray(testCase, chunkedArray, opts)
            arguments
                testCase
                chunkedArray
                opts.Type(1, 1) arrow.type.Type
                opts.NumChunks(1, 1) int32
                opts.Arrays(1, :) cell
            end
            testCase.assertTrue(numel(opts.Arrays) == opts.NumChunks); 
            allLengths = cellfun(@(a) a.Length, opts.Arrays, UniformOutput=true);
            expectedLengths = sum(allLengths, "native");

            testCase.verifyEqual(chunkedArray.NumChunks, opts.NumChunks);
            testCase.verifyEqual(chunkedArray.Length, expectedLengths);
            testCase.verifyEqual(chunkedArray.Type, opts.Type);

            for ii = 1:opts.NumChunks
                testCase.verifyEqual(chunkedArray.chunk(ii), opts.Arrays{ii});
            end
        end
    end
end
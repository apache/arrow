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

classdef tID < matlab.unittest.TestCase
% Test class for arrow.type.ID

    methods(TestClassSetup)
        function verifyOnMatlabPath(tc)
        % Verify the arrow array class is on the MATLAB Search Path.
            tc.assertTrue(~isempty(which("arrow.type.ID")), ...
                """arrow.type.ID"" must be on the MATLAB path. " + ...
                "Use ""addpath"" to add folders to the MATLAB path.");
        end
    end

    methods (Test)
        function bitWidth(testCase)
            import arrow.type.ID

            typeIDs = [ID.Boolean, ID.UInt8, ID.Int8, ID.UInt16, ...
                       ID.Int16, ID.UInt32, ID.Int32, ID.UInt64, ...
                       ID.Int64, ID.Float32, ID.Float64];

            expectedWidths = [1, 8, 8, 16, 16, 32, 32, 64, 64, 32, 64];

            for ii = 1:numel(typeIDs)
                actualWidth = bitWidth(typeIDs(ii)); 
                expectedWidth = expectedWidths(ii);
                testCase.verifyEqual(actualWidth, expectedWidth);
            end
        end

        function CastToUInt64(testCase)
            import arrow.type.ID

            typeIDs = [ID.Boolean, ID.UInt8, ID.Int8, ID.UInt16, ...
                       ID.Int16, ID.UInt32, ID.Int32, ID.UInt64, ...
                       ID.Int64, ID.Float32, ID.Float64];

            expectedValues = uint64([1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12]); 
            for ii = 1:numel(typeIDs)
                actualValue = uint64(typeIDs(ii)); 
                expectedValue = expectedValues(ii);
                testCase.verifyEqual(actualValue, expectedValue);
            end
        end
    end
end
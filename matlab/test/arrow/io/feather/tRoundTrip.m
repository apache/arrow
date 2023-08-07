%TROUNDTRIP Round trip tests for feather.

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
classdef tRoundTrip < matlab.unittest.TestCase

    methods(TestClassSetup)
        % Delete once arrow.internal.io.feather.Reader is submitted.
        function addFeatherFunctionsToMATLABPath(testCase)
            import matlab.unittest.fixtures.PathFixture
            % Add Feather test utilities to the MATLAB path.
            testCase.applyFixture(PathFixture('../../../util'));
            % arrow.cpp.call must be on the MATLAB path.
            testCase.assertTrue(~isempty(which('arrow.cpp.call')), ...
                '''arrow.cpp.call'' must be on the MATLAB path. Use ''addpath'' to add folders to the MATLAB path.');
        end
    end

    methods(Test)
        function Basic(testCase)
            import matlab.unittest.fixtures.TemporaryFolderFixture
            
            fixture = testCase.applyFixture(TemporaryFolderFixture);
            filename = fullfile(fixture.Folder, "temp.feather");

            DoubleVar = [10; 20; 30; 40];
            SingleVar = single([10; 15; 20; 25]);
            tWrite = table(DoubleVar, SingleVar);
            
            featherwrite(tWrite, filename);
            tRead = featherread(filename);
            testCase.verifyEqual(tWrite, tRead);
        end
    end
end

function featherwrite(T, filename)
    writer = arrow.internal.io.feather.Writer(filename);
    writer.write(T);
end

function T = featherread(filename)
    reader = arrow.internal.io.feather.Reader(filename);
    T = reader.read();
end
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
classdef tTimeUnit < matlab.unittest.TestCase
% Test class for arrow.type.timeUnit
    
    methods (Test)
        function Values(testCase)
        % Verify there are four TimeUnit enum values.
            import arrow.type.TimeUnit
            values = enumeration(TimeUnit.Second);
            expectedValues = [TimeUnit.Second, TimeUnit.Millisecond, ...
                              TimeUnit.Microsecond, TimeUnit.Nanosecond]';
            testCase.verifyEqual(values, expectedValues);
        end

        function TicksPerSecond(testCase)
        % Verify the TicksPerSecond property has the right value for each
        % TimeUnit value.
            import arrow.type.TimeUnit
            units = [TimeUnit.Second, TimeUnit.Millisecond, ...
                              TimeUnit.Microsecond, TimeUnit.Nanosecond]';
            ticksPerSecond = [1 1e3 1e6 1e9];
            for ii = 1:numel(units)
                testCase.verifyEqual(units(ii).TicksPerSecond, ticksPerSecond(ii));
            end
        end
    end
end


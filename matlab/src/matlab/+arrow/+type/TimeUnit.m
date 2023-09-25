% Enumeration class representing Time Units.

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
classdef TimeUnit < uint8

    enumeration
        Second       (0)
        Millisecond  (1)
        Microsecond  (2)
        Nanosecond   (3)
    end

    methods (Hidden)
        function ticks = ticksPerSecond(obj)
            import arrow.type.TimeUnit
            switch obj
                case TimeUnit.Second
                    ticks = 1;
                case TimeUnit.Millisecond
                    ticks = 1e3;
                case TimeUnit.Microsecond
                    ticks = 1e6;
                case TimeUnit.Nanosecond
                    ticks = 1e9;
            end
        end
    end
end
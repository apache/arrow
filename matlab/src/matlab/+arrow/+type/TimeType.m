%TIMETYPE Type class for time data.

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

classdef TimeType < arrow.type.TemporalType

    properties(Dependent, SetAccess=private, GetAccess=public)
        TimeUnit
    end

    methods
        function obj = TimeType(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy
            end
            obj@arrow.type.TemporalType(proxy);
        end

        function timeUnit = get.TimeUnit(obj)
            timeUnitValue = obj.Proxy.getTimeUnit();
            timeUnit = arrow.type.TimeUnit(timeUnitValue);
        end
    end

    methods (Access=protected)
        function groups = getDisplayPropertyGroups(~)
            targets = ["ID" "TimeUnit"];
            groups = matlab.mixin.util.PropertyGroup(targets);
        end
    end

    methods(Hidden)
        function data = preallocateMATLABArray(~, length)
            data = NaN([length 1]);
            data = seconds(data);
        end
    end

end
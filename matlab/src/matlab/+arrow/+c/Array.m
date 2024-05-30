%ARRAY Wrapper for an Arrow C Data Interface format ArrowArray C struct pointer.

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
classdef Array < matlab.mixin.Scalar

    properties (Hidden, SetAccess=private, GetAccess=public)
        Proxy
    end

    properties(Dependent, GetAccess=public, SetAccess=private)
        Address(1, 1) uint64
    end

    methods
        function obj = Array()
            proxyName = "arrow.c.proxy.Array";
            obj.Proxy = arrow.internal.proxy.create(proxyName);
        end

        function address = get.Address(obj)
            address = obj.Proxy.getAddress();
        end
    end
end
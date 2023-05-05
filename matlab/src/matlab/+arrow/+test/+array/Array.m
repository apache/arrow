classdef Array
    % Test class utility for allocating Arrow Arrays not backed by MATLAB
    % arrays.

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

    properties(Access = private)
        Proxy
    end

    properties (Dependent, GetAccess = public, SetAccess = private)
        ProxyID
    end

    methods
        function obj = Array(data)
            arguments
                % TODO: support non-numeric types
                data (1, :) {mustBeNumeric, mustBeReal, mustBeNonsparse}
            end
            % Copies data to create an Arrow Array.
            obj.Proxy = libmexclass.proxy.Proxy("Name", "arrow.test.array.proxy.Array", "ConstructorArguments", {data});
        end

        function id = get.ProxyID(obj)
            id = obj.Proxy.ID;
        end
    end
end
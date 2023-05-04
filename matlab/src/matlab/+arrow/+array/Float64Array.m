classdef Float64Array < matlab.mixin.CustomDisplay
    % arrow.array.Float64Array

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

    properties (Access=private)
        Proxy
    end

    properties (Access=private)
        MatlabArray
    end

    methods
        function obj = Float64Array(data, opts)
            arguments
                data
                opts.ID = false
            end

            if opts.ID
                validateattributes(data, "uint64", "scalar");
                obj.Proxy = libmexclass.proxy.Proxy("Name", "arrow.array.proxy.Float64Array", "ID", data);
            else
                validateattributes(data, "double", ["vector", "nonsparse", "real"]);
                obj.MatlabArray = data;
                obj.Proxy = libmexclass.proxy.Proxy("Name", "arrow.array.proxy.Float64Array", "ConstructorArguments", {obj.MatlabArray});
            end
        end

        function data = double(obj)
            data = obj.Proxy.ToMatlab();
        end
    end

    methods (Access=protected)
        function displayScalarObject(obj)
            disp(obj.ToString());
        end
    end

    methods (Access = private)
        function str = ToString(obj)
            str = obj.Proxy.ToString();
        end
    end
end

classdef (Abstract) Array < matlab.mixin.CustomDisplay & ...
                            matlab.mixin.Scalar
    % arrow.array.Array

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

    
    properties (Access=protected)
        Proxy
    end

    properties (Dependent)
        Length
        Valid % Validity bitmap
    end
    
    methods
        function obj = Array(varargin)
            obj.Proxy = libmexclass.proxy.Proxy(varargin{:}); 
        end

        function numElements = get.Length(obj)
            numElements = obj.Proxy.length();
        end

        function validElements = get.Valid(obj)
            validElements = obj.Proxy.valid();
        end

        function matlabArray = toMATLAB(obj)
            matlabArray = obj.Proxy.toMATLAB();
        end
    end

    methods (Access = private)
        function str = toString(obj)
            str = obj.Proxy.toString();
        end
    end

    methods (Access=protected)
        function displayScalarObject(obj)
            disp(obj.toString());
        end
    end
end


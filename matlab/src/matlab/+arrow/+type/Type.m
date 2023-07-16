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

classdef (Abstract) Type < matlab.mixin.CustomDisplay
%TYPE Abstract type class. 

    properties (Dependent, GetAccess=public, SetAccess=private)
        ID
        NumFields
    end

    properties (GetAccess=public, SetAccess=private, Hidden)
        Proxy
    end

    methods
        function obj = Type(varargin)
            obj.Proxy = libmexclass.proxy.Proxy(varargin{:}); 
        end

        function numFields = get.NumFields(obj)
            numFields = obj.Proxy.numFields();
        end

        function typeID = get.ID(obj)
            typeID = arrow.type.ID(obj.Proxy.typeID());
        end
    end

    methods (Access=protected)
        function propgrp = getPropertyGroups(~)
          proplist = {'ID'};
          propgrp = matlab.mixin.util.PropertyGroup(proplist);
        end
    end
    
end

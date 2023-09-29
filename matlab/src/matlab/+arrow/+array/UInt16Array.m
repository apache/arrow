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

classdef UInt16Array < arrow.array.NumericArray
% arrow.array.UInt16Array

    properties (Hidden, GetAccess=public, SetAccess=private)
        NullSubstitutionValue = uint16(0)
    end

    methods
        function obj = UInt16Array(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.array.proxy.UInt16Array")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.array.NumericArray(proxy);
        end

        function data = uint16(obj)
            data = obj.toMATLAB();
        end
    end

    methods (Static)
        function array = fromMATLAB(data, varargin)
            traits = arrow.type.traits.UInt16Traits;
            array = arrow.array.NumericArray.fromMATLAB(data, traits, varargin{:});
        end
    end
end

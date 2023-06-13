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

classdef NumericArray < arrow.array.Array
    % arrow.array.NumericArray
    
    
    properties (Hidden, SetAccess=protected)
        MatlabArray = []
    end

    properties(Abstract, Access=protected)
        NullSubstitutionValue;
    end

    methods
        function obj = NumericArray(data, type, proxyName, opts, nullOpts)
            arguments
                data
                type(1, 1) string
                proxyName(1, 1) string
                opts.DeepCopy(1, 1) logical = false
                nullOpts.InferNulls(1, 1) logical = true
                nullOpts.Valid
            end
            arrow.args.validateTypeAndShape(data, type);
            validElements = arrow.args.parseValidElements(data, nullOpts);
            obj@arrow.array.Array("Name", proxyName, "ConstructorArguments", {data, opts.DeepCopy, validElements});
            obj.MatlabArray = cast(obj.MatlabArray, type);
            % Store a reference to the array if not doing a deep copy
            if ~opts.DeepCopy, obj.MatlabArray = data; end
        end
        
        function matlabArray = toMATLAB(obj)
            matlabArray = obj.Proxy.toMATLAB();
            matlabArray(~obj.Valid) = obj.NullSubstitutionValue;
        end
    end
end


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

classdef BooleanArray < arrow.array.Array
% arrow.array.BooleanArray

    properties (Hidden, SetAccess=private)
        NullSubstitionValue = false;
    end

    methods
        function obj = BooleanArray(data, opts)
            arguments
                data
                opts.InferNulls(1,1) logical = true
                opts.Valid
            end
            arrow.args.validateTypeAndShape(data, "logical");
            validElements = arrow.args.parseValidElements(data, opts);
            obj@arrow.array.Array("Name", "arrow.array.proxy.BooleanArray", "ConstructorArguments", {data, validElements});
        end

        function data = logical(obj)
            data = obj.toMATLAB();
        end

        function matlabArray = toMATLAB(obj)
            matlabArray = obj.Proxy.toMATLAB();
            matlabArray(~obj.Valid) = obj.NullSubstitionValue;
        end
    end
end

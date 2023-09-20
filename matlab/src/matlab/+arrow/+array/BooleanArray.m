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

    properties (Hidden, GetAccess=public, SetAccess=private)
        NullSubstitutionValue = false;
    end

    methods
        function obj = BooleanArray(proxy)
             arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.array.proxy.BooleanArray")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.array.Array(proxy);
        end

        function data = logical(obj)
            data = obj.toMATLAB();
        end

        function matlabArray = toMATLAB(obj)
            matlabArray = obj.Proxy.toMATLAB();
            matlabArray(~obj.Valid) = obj.NullSubstitutionValue;
        end
    end

    methods (Static)
        function array = fromMATLAB(data, opts)
            arguments
                data
                opts.InferNulls(1, 1) logical = true
                opts.Valid
            end

            arrow.internal.validate.type(data, "logical");
            arrow.internal.validate.shape(data);
            arrow.internal.validate.nonsparse(data);
            validElements = arrow.internal.validate.parseValidElements(data, opts);
            
            args = struct(MatlabArray=data, Valid=validElements);
            proxy = arrow.internal.proxy.create("arrow.array.proxy.BooleanArray", args);
            array = arrow.array.BooleanArray(proxy);
        end
    end
end

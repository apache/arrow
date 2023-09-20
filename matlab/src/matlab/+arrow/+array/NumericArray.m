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

    properties(Abstract, Hidden, GetAccess=public, SetAccess=private)
        NullSubstitutionValue;
    end

    methods
        function obj = NumericArray(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy
            end
            obj@arrow.array.Array(proxy);
        end

        function matlabArray = toMATLAB(obj)
            matlabArray = obj.Proxy.toMATLAB();
            matlabArray(~obj.Valid) = obj.NullSubstitutionValue;
        end
    end

    methods (Static)
        function array = fromMATLAB(data, traits, opts)
            arguments
                data
                traits(1, 1) arrow.type.traits.TypeTraits
                opts.InferNulls(1, 1) logical = true
                opts.Valid
            end

            arrow.internal.validate.numeric(data, traits.MatlabClassName);
            validElements = arrow.internal.validate.parseValidElements(data, opts);
            args = struct(MatlabArray=data, Valid=validElements);
            proxy = arrow.internal.proxy.create(traits.ArrayProxyClassName, args);
            array = traits.ArrayConstructor(proxy);
        end
    end
end


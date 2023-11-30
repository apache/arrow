% arrow.array.Time32Array

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

classdef Time32Array < arrow.array.Array

    properties (Hidden, GetAccess=public, SetAccess=private)
        NullSubstitutionValue = seconds(NaN);
    end

    methods
        function obj = Time32Array(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.array.proxy.Time32Array")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.array.Array(proxy);
        end

        function times = toMATLAB(obj)
            import arrow.type.TimeUnit

            matlabArray = obj.Proxy.toMATLAB();
            if obj.Type.TimeUnit == TimeUnit.Second
                times = seconds(matlabArray);
            else
                times = milliseconds(matlabArray);
            end
            times(~obj.Valid) = obj.NullSubstitutionValue;
        end

        function times = duration(obj)
            times = obj.toMATLAB();
        end
    end

    methods(Static, Access=private)
        function ticks = convertDurationToTicks(data, timeUnit)
            if (timeUnit == arrow.type.TimeUnit.Second)
                ticks = cast(seconds(data), "int32");
            else
                ticks = cast(milliseconds(data), "int32");
            end
        end
    end

    methods(Static)
        function array = fromMATLAB(data, opts)
            arguments
                data
                opts.TimeUnit(1, 1) TimeUnit {timeUnit("Time32", opts.TimeUnit)} = TimeUnit.Second
                opts.InferNulls(1, 1) logical = true
                opts.Valid
            end

            import arrow.type.TimeUnit
            import arrow.array.Time32Array
            import arrow.internal.validate.temporal.timeUnit
            
            arrow.internal.validate.type(data, "duration");
            arrow.internal.validate.shape(data);

            validElements = arrow.internal.validate.parseValidElements(data, opts);
            ticks = Time32Array.convertDurationToTicks(data, opts.TimeUnit);

            args = struct(MatlabArray=ticks, Valid=validElements, TimeUnit=string(opts.TimeUnit));
            proxy = arrow.internal.proxy.create("arrow.array.proxy.Time32Array", args);
            array = Time32Array(proxy);
        end
    end
end
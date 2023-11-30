% arrow.array.Time64Array

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

classdef Time64Array < arrow.array.Array

    properties (Hidden, GetAccess=public, SetAccess=private)
        NullSubstitutionValue = seconds(NaN);
    end

    methods
        function obj = Time64Array(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.array.proxy.Time64Array")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.array.Array(proxy);
        end

        function times = toMATLAB(obj)
            import arrow.type.TimeUnit
            divider = ticksPerSecond(obj.Type.TimeUnit) / ticksPerSecond(TimeUnit.Millisecond);
            matlabArray = obj.Proxy.toMATLAB();
            % TODO: This conversion may be lossy. Is it better to cast the
            % int64 array to a double before dividing by 1e3 or 1e6? 
            times = milliseconds(cast(matlabArray, "double") / divider);
            times(~obj.Valid) = obj.NullSubstitutionValue;
        end

        function times = duration(obj)
            times = obj.toMATLAB();
        end
    end

    methods(Static, Access=private)
        function ticks = convertDurationToTicks(data, timeUnit)
            import arrow.type.TimeUnit
            multiplier = ticksPerSecond(timeUnit) / ticksPerSecond(TimeUnit.Millisecond);
            ticks = cast(milliseconds(data) * multiplier, "int64");
        end
    end

    methods(Static)
        function array = fromMATLAB(data, opts)
            arguments
                data
                opts.TimeUnit(1, 1) TimeUnit {timeUnit("Time64", opts.TimeUnit)} = TimeUnit.Microsecond
                opts.InferNulls(1, 1) logical = true
                opts.Valid
            end

            import arrow.type.TimeUnit
            import arrow.array.Time64Array
            import arrow.internal.validate.temporal.timeUnit
            
            arrow.internal.validate.type(data, "duration");
            arrow.internal.validate.shape(data);

            validElements = arrow.internal.validate.parseValidElements(data, opts);
            ticks = Time64Array.convertDurationToTicks(data, opts.TimeUnit);

            args = struct(MatlabArray=ticks, Valid=validElements, TimeUnit=string(opts.TimeUnit));
            proxy = arrow.internal.proxy.create("arrow.array.proxy.Time64Array", args);
            array = Time64Array(proxy);
        end
    end
end
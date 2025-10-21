% arrow.array.Date64Array

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

classdef Date64Array < arrow.array.Array

    properties(Hidden, GetAccess=public, SetAccess=private)
        NullSubstitutionValue = NaT
    end

    methods

        function obj = Date64Array(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.array.proxy.Date64Array")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.array.Array(proxy);
        end

        function dates = toMATLAB(obj)
            epochTime = obj.Proxy.toMATLAB();

            ticksPerSecond = obj.Type.DateUnit.ticksPerSecond();

            % UNIX Epoch (January 1st, 1970).
            unixEpoch = datetime(0, ConvertFrom="posixtime", TimeZone="UTC");
            dates = datetime(epochTime, ConvertFrom="epochtime", Epoch=unixEpoch, ....
                TicksPerSecond=ticksPerSecond);

            dates(~obj.Valid) = obj.NullSubstitutionValue;
        end

        function dates = datetime(obj)
            dates = obj.toMATLAB();
        end

    end

    methods(Static)

        function array = fromMATLAB(data, opts)
            arguments
                data
                opts.InferNulls(1, 1) logical = true
                opts.Valid
            end

            import arrow.array.Date64Array

            arrow.internal.validate.type(data, "datetime");
            arrow.internal.validate.shape(data);

            validElements = arrow.internal.validate.parseValidElements(data, opts);
            epochTime = arrow.array.internal.temporal.convertDatetimeToEpochTime(data, arrow.type.DateUnit.Millisecond);

            args = struct(MatlabArray=epochTime, Valid=validElements);
            proxy = arrow.internal.proxy.create("arrow.array.proxy.Date64Array", args);
            array = Date64Array(proxy);
        end

    end

end

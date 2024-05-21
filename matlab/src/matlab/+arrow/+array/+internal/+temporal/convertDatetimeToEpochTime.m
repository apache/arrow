% Converts MATLAB datetime values to integer "Epoch time" values which
% represent the number of "ticks" since the UNIX Epoch (Jan-1-1970) with
% respect to the specified TimeUnit / DateUnit.

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

function epochTime = convertDatetimeToEpochTime(datetimes, unit)
    epochTime = zeros(size(datetimes), "int64");
    indices = ~isnat(datetimes);

    % convertTo uses the Unzoned UNIX Epoch Jan-1-1970 as the default Epoch.
    % If the input datetime array has a TimeZone, then a Zoned UNIX Epoch
    % of Jan-1-1970 UTC is used instead.
    %
    % TODO: convertTo may error if the datetime is 2^63-1 before or
    % after the epoch. We should throw a custom error in this case.
    epochTime(indices) = convertTo(datetimes(indices), "epochtime", TicksPerSecond=ticksPerSecond(unit));
end

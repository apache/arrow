%NUMERIC Validates data is a real, nonsparse, numeric array that is
%either a vector or an empty array.

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
function numeric(data, type)
    arrow.internal.validate.type(data, type);
    arrow.internal.validate.shape(data);
    arrow.internal.validate.nonsparse(data);
    arrow.internal.validate.realnumeric(data);
end


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

function validateTypeAndShape(data, type)
    % Validates data has the expected type and is a vector or empty 2D
    % matrix. If data is numeric, validates is real and nonsparse.

    arguments
        data
        type(1, 1) string
    end

    % If data is empty, only require it's shape to be 2D to support 0x0 
    % arrays. Otherwise, require data to be a vector.
    %
    % TODO: Consider supporting nonvector 2D arrays. We chould reshape them
    % to column vectors if needed.
    
    expectedShape = "vector";
    if isempty(data)
        expectedShape = "2d";
    end
    validateattributes(data, type, [expectedShape, "nonsparse", "real"]);
end
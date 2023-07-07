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

function typeTraits = traits(type)
    arguments
        type(1,1) arrow.type.Type
    end
    
    import arrow.type.traits.*
    typeClass = string(class(type));
    
    switch typeClass
        case "arrow.type.UInt8Type"
            typeTraits = UInt8Traits();
        case "arrow.type.UInt16Type"
            typeTraits = UInt16Traits();
        case "arrow.type.UInt32Type"
            typeTraits = UInt32Traits();
        case "arrow.type.UInt64Type"
            typeTraits = UInt64Traits();
        case "arrow.type.Int8Type"
            typeTraits = Int8Traits();
        case "arrow.type.Int16Type"
            typeTraits = Int16Traits();
        case "arrow.type.Int32Type"
            typeTraits = Int32Traits();
        case "arrow.type.Int64Type"
            typeTraits = Int64Traits();
        case "arrow.type.Float32Type"
            typeTraits = Float32Traits();
        case "arrow.type.Float64Type"
            typeTraits = Float64Traits();
        case "arrow.type.BooleanType"
            typeTraits = BooleanTraits();
        case "arrow.type.StringType"
            typeTraits = StringTraits();
        case "arrow.type.TimestampType"
            typeTraits = TimestampTraits();
        otherwise
            error("arrow:type:traits:UnknownType", "Unknown type: " + typeClass);
    end
end
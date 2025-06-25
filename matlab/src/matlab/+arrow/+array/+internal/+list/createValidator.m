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

function validator = createValidator(data)
    import arrow.array.internal.list.ClassTypeValidator
    import arrow.array.internal.list.DatetimeValidator
    import arrow.array.internal.list.TableValidator

    if isnumeric(data)
        validator = ClassTypeValidator(data);
    elseif islogical(data)
        validator = ClassTypeValidator(data);
    elseif isduration(data)
        validator = ClassTypeValidator(data);
    elseif isstring(data)
        validator = ClassTypeValidator(data);
    elseif iscell(data)
        validator = ClassTypeValidator(data);
    elseif isdatetime(data)
        validator = DatetimeValidator(data);
    elseif istable(data)
        validator = TableValidator(data);
    else
        errorID = "arrow:array:list:UnsupportedDataType";
        msg = "Unable to create a ListArray from a cell array containing " + class(data) + " values.";
        error(errorID, msg);
    end

end


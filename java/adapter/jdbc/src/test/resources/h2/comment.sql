--Licensed to the Apache Software Foundation (ASF) under one or more contributor
--license agreements. See the NOTICE file distributed with this work for additional
--information regarding copyright ownership. The ASF licenses this file to
--You under the Apache License, Version 2.0 (the "License"); you may not use
--this file except in compliance with the License. You may obtain a copy of
--the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
--by applicable law or agreed to in writing, software distributed under the
--License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
--OF ANY KIND, either express or implied. See the License for the specific
--language governing permissions and limitations under the License.
create table table1(
  id bigint primary key,
  name varchar(255),
  column1 boolean,
  columnN int
  );

COMMENT ON TABLE table1 IS 'This is super special table with valuable data';
COMMENT ON COLUMN table1.id IS 'Record identifier';
COMMENT ON COLUMN table1.name IS 'Name of record';
COMMENT ON COLUMN table1.columnN IS 'Informative description of columnN';
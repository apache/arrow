#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
OUTFILE=data_model.dot

license() {
  cat <<'LICENSE' > ${1}
/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.See the License for the
 specific language governing permissions and limitations
 under the License.
*/

LICENSE
}

warning() {
  cat <<'WARNING' >> ${1}
/*
 WARNING
   This is an auto-generated file. Please do not edit.

   To reproduce, please run :code:`./make_data_model_rst.sh`.
   (This requires you have the
   `psql client <https://www.postgresql.org/download/>`_
   and have started the docker containers using
   :code:`docker-compose up`).
*/
WARNING
}

echo "Making ${OUTFILE}"

license ${OUTFILE}
warning ${OUTFILE}

PGPASSWORD=arrow \
  psql --tuples-only --username=arrow_web \
  --dbname=benchmark --port=5432 --host=localhost \
  --command="select public.documentation_dotfile();" \
  | sed "s/ *+$//" | sed "s/^ //" >> ${OUTFILE}

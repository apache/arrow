#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

# check that optional pyarrow modules are available
# because pytest would just skip the substrait tests
echo "Substrait Integration Tests"
echo "Validating imports"
python -c "import pyarrow.substrait"
python -c "from substrait_consumer.consumers import AceroConsumer"

echo "Executing pytest"
cd consumer-testing
pytest substrait_consumer/tests/functional/extension_functions/test_boolean_functions.py --producer IsthmusProducer --consumer AceroConsumer

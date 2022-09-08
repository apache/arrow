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


import os
import shutil
import random

import pandas as pd

if __name__ == "__main__":
    # generate the test dataframe
    data = {
        "total_amount": list(),
        "fare_amount": list()
    }
    for i in range(0, 500):
        data['total_amount'].append(random.randint(1,11)*5)
        data['fare_amount'].append(random.randint(1,11)*3)
    df = pd.DataFrame(data)

    # dump the dataframe to a parquet file
    df.to_parquet("skyhook_test_data.parquet")

    # create the dataset by copying the parquet files
    shutil.rmtree("nyc", ignore_errors=True)
    payment_type = ["1", "2", "3", "4"]
    vendor_id = ["1", "2"]
    for p in payment_type:
        for v in vendor_id:
            path = f"nyc/payment_type={p}/VendorID={v}"
            os.makedirs(path, exist_ok=True)
            shutil.copyfile("skyhook_test_data.parquet", os.path.join(path, f"{p}.{v}.parquet"))

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef WRAPPED_KEY_STORE_H_
#define WRAPPED_KEY_STORE_H_

#include "parquet/crypto_keytools/kms_client.h"

#include <string>

#include "parquet/util/visibility.h"

namespace parquet {

class PARQUET_EXPORT WrappedKeyStore {
 public:
  virtual void storeWrappedKey(std::string wrapped_key, std::string file_id, std::string key_id_in_file) = 0;
  virtual std::string getWrappedKey(std::string file_id, std::string key_id_in_file) = 0;
};

} // namespace parquet


#endif /* WRAPPED_KEY_STORE_H_ */

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

#include <iostream>
#include <rados/librados.hpp>
#include <sstream>
#include <string>

namespace librados {

// the below functions are taken from ceph src tree:
// https://github.com/ceph/ceph/blob/master/src/test/librados/test_shared.h

std::string get_temp_pool_name(const std::string& prefix = "test-rados-api-");
std::string create_one_pool_pp(const std::string& pool_name, librados::Rados& cluster);
std::string create_one_pool_pp(const std::string& pool_name, librados::Rados& cluster,
                               const std::map<std::string, std::string>& config);
std::string create_one_ec_pool_pp(const std::string& pool_name, librados::Rados& cluster);
std::string connect_cluster_pp(librados::Rados& cluster);
std::string connect_cluster_pp(librados::Rados& cluster,
                               const std::map<std::string, std::string>& config);
int destroy_one_pool_pp(const std::string& pool_name, librados::Rados& cluster);

}  // namespace librados

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

#ifndef JNI_MODULE_HOLDER_H
#define JNI_MODULE_HOLDER_H

#include <memory>
#include <utility>

#include "gandiva/arrow.h"

namespace gandiva {

class Projector;
class Filter;

class ProjectorHolder {
 public:
  ProjectorHolder(SchemaPtr schema, FieldVector ret_types,
                  std::shared_ptr<Projector> projector)
      : schema_(schema), ret_types_(ret_types), projector_(std::move(projector)) {}

  SchemaPtr schema() { return schema_; }
  FieldVector rettypes() { return ret_types_; }
  std::shared_ptr<Projector> projector() { return projector_; }

 private:
  SchemaPtr schema_;
  FieldVector ret_types_;
  std::shared_ptr<Projector> projector_;
};

class FilterHolder {
 public:
  FilterHolder(SchemaPtr schema, std::shared_ptr<Filter> filter)
      : schema_(schema), filter_(std::move(filter)) {}

  SchemaPtr schema() { return schema_; }
  std::shared_ptr<Filter> filter() { return filter_; }

 private:
  SchemaPtr schema_;
  std::shared_ptr<Filter> filter_;
};

}  // namespace gandiva

#endif  // JNI_MODULE_HOLDER_H

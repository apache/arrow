// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>

#ifndef GANDIVA_GENERATE_DATA_H
#define GANDIVA_GENERATE_DATA_H

namespace gandiva {

template <typename C_TYPE>
class DataGenerator {
 public:
  virtual C_TYPE GenerateData() = 0;
};

class Int32DataGenerator : public DataGenerator<int32_t> {
 public:
  Int32DataGenerator() : seed_(100) {}

  int32_t GenerateData() { return rand_r(&seed_); }

 protected:
  unsigned int seed_;
};

class BoundedInt32DataGenerator : public Int32DataGenerator {
 public:
  explicit BoundedInt32DataGenerator(uint32_t upperBound)
      : Int32DataGenerator(), upperBound_(upperBound) {}

  int32_t GenerateData() { return (rand_r(&seed_) % upperBound_); }

 protected:
  uint32_t upperBound_;
};

class Int64DataGenerator : public DataGenerator<int64_t> {
 public:
  Int64DataGenerator() : seed_(100) {}

  int64_t GenerateData() { return rand_r(&seed_); }

 protected:
  unsigned int seed_;
};

}  // namespace gandiva

#endif  // GANDIVA_GENERATE_DATA_H

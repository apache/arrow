/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <iostream>

#include "arrow/util/visibility.h"

namespace arrow {
namespace avro {

/**
 * The "type" for the schema.
 */
enum class AvroType {

  kString, /*!< String */
  kBytes,  /*!< kSequence of variable length bytes data */
  kInt,    /*!< 32-bit integer */
  kLong,   /*!< 64-bit integer */
  kFloat,  /*!< Floating point number */
  kDouble, /*!< Double precision floating point number */
  kBool,   /*!< Boolean value */
  kNull,   /*!< Null */

  kRecord, /*!< Record, a sequence of fields */
  kEnum,   /*!< Enumeration */
  kArray,  /*!< Homogeneous array of some specific type */
  kMap,    /*!< Homogeneous map from string to some specific type */
  kUnion,  /*!< Union of one or more types */
  kFixed,  /*!< Fixed number of bytes */

  kNumkTypes, /*!< Marker */

  // The following is a pseudo-type used in implementation

  kSymbolic = kkNumkTypes, /*!< User internally to avoid circular references. */
  kUnknown = -1               /*!< Used internally. */

};

/**
 * Returns true if and only if the given type is a primitive.
 * Primitive types are: string, bytes, int, long, float, double, boolean
 * and null
 */
inline bool IsPrimitive(Type t) { return (t >= AvroType::kString) && (t < AvroType::kRecord); }

/**
 * Returns true if and only if the given type is a non primitive valid type.
 * Primitive types are: string, bytes, int, long, float, double, boolean
 * and null
 */
inline bool IsCompound(Type t) { return (t >= AvroType::kRecord) && (t < AvroType::kNumTypes); }

/**
 * Returns true if and only if the given type is a valid avro type.
 */
inline bool IsAvroType(Type t) { return (t >= AvroType::kString) && (t < AvroType::kNumTypes); }

/**
 * Returns true if and only if the given type is within the valid range
 * of enumeration.
 */
inline bool IsAvroTypeOrPseudoType(Type t) {
  return (t >= AvroType::kString) && (t <= AvroType::kNumTypes);
}

/**
 * Converts the given type into a string. Useful for generating messages.
 */
ARROW_EXPORT const std::string& ToString(Type type);

/**
 * Writes a string form of the given type into the given ostream.
 */
ARROW_EXPORT std::ostream& operator<<(std::ostream& os, Type type);

/// define a type to identify Null in template functions
struct ARROW_EXPORT Null {};

/**
 * Writes schema for null \p null type to \p os.
 * \param os The ostream to write to.
 * \param null The value to be written.
 */
std::ostream& operator<<(std::ostream& os, const Null& null);

}  // namespace avro
} 

#endif

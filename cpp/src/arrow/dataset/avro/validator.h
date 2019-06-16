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

#ifndef avro_Validating_hh__
#define avro_Validating_hh__

#include <boost/noncopyable.hpp>
#include <vector>
#include <stdint.h>

#include "Config.hh"
#include "Types.hh"
#include "ValidSchema.hh"

namespace avro {

class AVRO_DECL NullValidator : private boost::noncopyable
{
  public:

    explicit NullValidator(const ValidSchema &schema) {}
    NullValidator() {}

    void setCount(int64_t val) {}

    bool typeIsExpected(Type type) const {
        return true;
    }

    Type nextTypeExpected() const {
        return AVRO_UNKNOWN;
    }

    int nextSizeExpected() const {
        return 0;
    }

    bool getCurrentRecordName(std::string &name) const {
        return true;
    }

    bool getNextFieldName(std::string &name) const {
        return true;
    }

    void checkTypeExpected(Type type) { }
    void checkFixedSizeExpected(int size) { }


};

/// This class is used by both the ValidatingSerializer and ValidationParser
/// objects.  It advances the parse tree (containing logic how to advance
/// through the various compound types, for example a record must advance
/// through all leaf nodes but a union only skips to one), and reports which
/// type is next.

class AVRO_DECL Validator : private boost::noncopyable
{
  public:

    explicit Validator(const ValidSchema &schema);

    void setCount(int64_t val);

    bool typeIsExpected(Type type) const {
        return (expectedTypesFlag_ & typeToFlag(type)) != 0;
    }

    Type nextTypeExpected() const {
        return nextType_;
    }

    int nextSizeExpected() const;

    bool getCurrentRecordName(std::string &name) const;
    bool getNextFieldName(std::string &name) const;

    void checkTypeExpected(Type type) {
        if(! typeIsExpected(type)) {
            throw Exception(
                boost::format("Type %1% does not match schema %2%") 
                    % type % nextType_
            );
        }
        advance();
    }

    void checkFixedSizeExpected(int size) { 
        if( nextSizeExpected() != size) {
            throw Exception(
                boost::format("Wrong size for fixed, got %1%, expected %2%") 
                    % size % nextSizeExpected()
            );
        }
        checkTypeExpected(AVRO_FIXED);
    }

  private:

    typedef uint32_t flag_t;

    flag_t typeToFlag(Type type) const {
        flag_t flag = (1L << type);
        return flag;
    }

    void setupOperation(const NodePtr &node);

    void setWaitingForCount();

    void advance();
    void doAdvance();

    void enumAdvance();
    bool countingSetup();
    void countingAdvance();
    void unionAdvance();
    void fixedAdvance();

    void setupFlag(Type type);

    const ValidSchema schema_;

    Type nextType_; 
    flag_t expectedTypesFlag_;
    bool compoundStarted_;
    bool waitingForCount_;
    int64_t count_;

    struct CompoundType {
        explicit CompoundType(const NodePtr &n) :
            node(n), pos(0)
        {}
        NodePtr node;  ///< save the node
        size_t  pos;   ///< track the leaf position to visit
    };

    std::vector<CompoundType> compoundStack_;
    std::vector<size_t> counters_;

};

} // namespace avro

#endif

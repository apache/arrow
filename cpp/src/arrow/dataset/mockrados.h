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

#pragma once

#include "arrow/dataset/rados.h"

#include <gmock/gmock.h>

#include "arrow/dataset/api.h"
#include "arrow/dataset/visibility.h"
#include "arrow/util/macros.h"
#include "arrow/ipc/api.h"
#include "arrow/api.h"

namespace arrow {
namespace dataset {

ARROW_DS_EXPORT std::shared_ptr<RecordBatch> generate_test_record_batch();

ARROW_DS_EXPORT std::shared_ptr<Table> generate_test_table();

ARROW_DS_EXPORT Status get_test_table_in_bufferlist(librados::bufferlist &bl);

class ARROW_DS_EXPORT MockIoCtx : public IoCtxInterface {
public:
    MockIoCtx(){
        this->setup();
        testing::Mock::AllowLeak(this);
    }
    ~MockIoCtx(){}
    MOCK_METHOD2(write_full, int(const std::string& oid, librados::bufferlist& bl));
    MOCK_METHOD4(read, int(const std::string& oid, librados::bufferlist& bl, size_t len, uint64_t off));
    MOCK_METHOD5(exec, int(const std::string& oid, const char *cls, const char *method, librados::bufferlist& inbl, librados::bufferlist& outbl));
    MOCK_METHOD1(setIoCtx, void (librados::IoCtx *ioCtx_));
private:
    Status setup(){
        EXPECT_CALL(*this, write_full(testing::_, testing::_))
                .WillOnce(testing::Return(0));

        librados::bufferlist result;
        get_test_table_in_bufferlist(result);

        EXPECT_CALL(*this, read(testing::_, testing::_, testing::_, testing::_))
                .WillOnce(DoAll(testing::SetArgReferee<1>(result), testing::Return(0)));

        EXPECT_CALL(*this, exec(testing::_, testing::_, testing::_, testing::_, testing::_))
                .WillRepeatedly(DoAll(testing::SetArgReferee<4>(result), testing::Return(0)));
        return Status::OK();
    }
};

class ARROW_DS_EXPORT MockRados : public RadosInterface {
public:
    MockRados(){
        this->setup();
        testing::Mock::AllowLeak(this);
    }
    ~MockRados(){}
    MOCK_METHOD3(init2, int(const char * const name, const char * const clustername, uint64_t flags));
    MOCK_METHOD2(ioctx_create, int(const char *name, IoCtxInterface *pioctx));
    MOCK_METHOD1(conf_read_file, int(const char * const path));
    MOCK_METHOD0(connect, int());
    MOCK_METHOD0(shutdown, void());
private:
    Status setup(){
        EXPECT_CALL(*this, init2(testing::_, testing::_, testing::_))
                .WillOnce(testing::Return(0));
        EXPECT_CALL(*this, ioctx_create(testing::_,testing::_))
                .WillOnce(testing::Return(0));
        EXPECT_CALL(*this, conf_read_file(testing::_))
                .WillOnce(testing::Return(0));
        EXPECT_CALL(*this, connect())
                .WillOnce(testing::Return(0));
        EXPECT_CALL(*this, shutdown())
                .WillRepeatedly(testing::Return());
        return Status::OK();
    }
};

} // namespace arrow
} // namespace dataset
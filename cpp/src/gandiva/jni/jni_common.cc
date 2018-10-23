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

#include <google/protobuf/io/coded_stream.h>

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "Types.pb.h"
#include "gandiva/configuration.h"
#include "gandiva/filter.h"
#include "gandiva/jni/config_holder.h"
#include "gandiva/jni/env_helper.h"
#include "gandiva/jni/id_to_module_map.h"
#include "gandiva/jni/module_holder.h"
#include "gandiva/projector.h"
#include "gandiva/tree_expr_builder.h"
#include "jni/org_apache_arrow_gandiva_evaluator_JniWrapper.h"

using gandiva::ConditionPtr;
using gandiva::DataTypePtr;
using gandiva::ExpressionPtr;
using gandiva::ExpressionVector;
using gandiva::FieldPtr;
using gandiva::FieldVector;
using gandiva::Filter;
using gandiva::NodePtr;
using gandiva::NodeVector;
using gandiva::Projector;
using gandiva::SchemaPtr;
using gandiva::Status;
using gandiva::TreeExprBuilder;

using gandiva::ArrayDataVector;
using gandiva::ConfigHolder;
using gandiva::Configuration;
using gandiva::ConfigurationBuilder;
using gandiva::FilterHolder;
using gandiva::ProjectorHolder;

// forward declarations
NodePtr ProtoTypeToNode(const types::TreeNode& node);

static jint JNI_VERSION = JNI_VERSION_1_6;

// extern refs - initialized for other modules.
jclass configuration_builder_class_;
jmethodID byte_code_accessor_method_id_;

// refs for self.
static jclass gandiva_exception_;

// module maps
gandiva::IdToModuleMap<std::shared_ptr<ProjectorHolder>> projector_modules_;
gandiva::IdToModuleMap<std::shared_ptr<FilterHolder>> filter_modules_;

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
  jclass local_configuration_builder_class_ =
      env->FindClass("org/apache/arrow/gandiva/evaluator/ConfigurationBuilder");
  configuration_builder_class_ =
      (jclass)env->NewGlobalRef(local_configuration_builder_class_);
  env->DeleteLocalRef(local_configuration_builder_class_);

  jclass localExceptionClass =
      env->FindClass("org/apache/arrow/gandiva/exceptions/GandivaException");
  gandiva_exception_ = (jclass)env->NewGlobalRef(localExceptionClass);
  env->DeleteLocalRef(localExceptionClass);

  const char method_name[] = "getByteCodeFilePath";
  const char return_type[] = "()Ljava/lang/String;";
  byte_code_accessor_method_id_ =
      env->GetMethodID(configuration_builder_class_, method_name, return_type);

  env->ExceptionDescribe();

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  env->DeleteGlobalRef(configuration_builder_class_);
  env->DeleteGlobalRef(gandiva_exception_);
}

DataTypePtr ProtoTypeToTime32(const types::ExtGandivaType& ext_type) {
  switch (ext_type.timeunit()) {
    case types::SEC:
      return arrow::time32(arrow::TimeUnit::SECOND);
    case types::MILLISEC:
      return arrow::time32(arrow::TimeUnit::MILLI);
    default:
      std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for time32\n";
      return nullptr;
  }
}

DataTypePtr ProtoTypeToTime64(const types::ExtGandivaType& ext_type) {
  switch (ext_type.timeunit()) {
    case types::MICROSEC:
      return arrow::time64(arrow::TimeUnit::MICRO);
    case types::NANOSEC:
      return arrow::time64(arrow::TimeUnit::NANO);
    default:
      std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for time64\n";
      return nullptr;
  }
}

DataTypePtr ProtoTypeToTimestamp(const types::ExtGandivaType& ext_type) {
  switch (ext_type.timeunit()) {
    case types::SEC:
      return arrow::timestamp(arrow::TimeUnit::SECOND);
    case types::MILLISEC:
      return arrow::timestamp(arrow::TimeUnit::MILLI);
    case types::MICROSEC:
      return arrow::timestamp(arrow::TimeUnit::MICRO);
    case types::NANOSEC:
      return arrow::timestamp(arrow::TimeUnit::NANO);
    default:
      std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for timestamp\n";
      return nullptr;
  }
}

DataTypePtr ProtoTypeToDataType(const types::ExtGandivaType& ext_type) {
  switch (ext_type.type()) {
    case types::NONE:
      return arrow::null();
    case types::BOOL:
      return arrow::boolean();
    case types::UINT8:
      return arrow::uint8();
    case types::INT8:
      return arrow::int8();
    case types::UINT16:
      return arrow::uint16();
    case types::INT16:
      return arrow::int16();
    case types::UINT32:
      return arrow::uint32();
    case types::INT32:
      return arrow::int32();
    case types::UINT64:
      return arrow::uint64();
    case types::INT64:
      return arrow::int64();
    case types::HALF_FLOAT:
      return arrow::float16();
    case types::FLOAT:
      return arrow::float32();
    case types::DOUBLE:
      return arrow::float64();
    case types::UTF8:
      return arrow::utf8();
    case types::BINARY:
      return arrow::binary();
    case types::DATE32:
      return arrow::date32();
    case types::DATE64:
      return arrow::date64();
    case types::DECIMAL:
      // TODO: error handling
      return arrow::decimal(ext_type.precision(), ext_type.scale());
    case types::TIME32:
      return ProtoTypeToTime32(ext_type);
    case types::TIME64:
      return ProtoTypeToTime64(ext_type);
    case types::TIMESTAMP:
      return ProtoTypeToTimestamp(ext_type);

    case types::FIXED_SIZE_BINARY:
    case types::INTERVAL:
    case types::LIST:
    case types::STRUCT:
    case types::UNION:
    case types::DICTIONARY:
    case types::MAP:
      std::cerr << "Unhandled data type: " << ext_type.type() << "\n";
      return nullptr;

    default:
      std::cerr << "Unknown data type: " << ext_type.type() << "\n";
      return nullptr;
  }
}

FieldPtr ProtoTypeToField(const types::Field& f) {
  const std::string& name = f.name();
  DataTypePtr type = ProtoTypeToDataType(f.type());
  bool nullable = true;
  if (f.has_nullable()) {
    nullable = f.nullable();
  }

  return field(name, type, nullable);
}

NodePtr ProtoTypeToFieldNode(const types::FieldNode& node) {
  FieldPtr field_ptr = ProtoTypeToField(node.field());
  if (field_ptr == nullptr) {
    std::cerr << "Unable to create field node from protobuf\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeField(field_ptr);
}

NodePtr ProtoTypeToFnNode(const types::FunctionNode& node) {
  const std::string& name = node.functionname();
  NodeVector children;

  for (int i = 0; i < node.inargs_size(); i++) {
    const types::TreeNode& arg = node.inargs(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == nullptr) {
      std::cerr << "Unable to create argument for function: " << name << "\n";
      return nullptr;
    }

    children.push_back(n);
  }

  DataTypePtr return_type = ProtoTypeToDataType(node.returntype());
  if (return_type == nullptr) {
    std::cerr << "Unknown return type for function: " << name << "\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeFunction(name, children, return_type);
}

NodePtr ProtoTypeToIfNode(const types::IfNode& node) {
  NodePtr cond = ProtoTypeToNode(node.cond());
  if (cond == nullptr) {
    std::cerr << "Unable to create cond node for if node\n";
    return nullptr;
  }

  NodePtr then_node = ProtoTypeToNode(node.thennode());
  if (then_node == nullptr) {
    std::cerr << "Unable to create then node for if node\n";
    return nullptr;
  }

  NodePtr else_node = ProtoTypeToNode(node.elsenode());
  if (else_node == nullptr) {
    std::cerr << "Unable to create else node for if node\n";
    return nullptr;
  }

  DataTypePtr return_type = ProtoTypeToDataType(node.returntype());
  if (return_type == nullptr) {
    std::cerr << "Unknown return type for if node\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeIf(cond, then_node, else_node, return_type);
}

NodePtr ProtoTypeToAndNode(const types::AndNode& node) {
  NodeVector children;

  for (int i = 0; i < node.args_size(); i++) {
    const types::TreeNode& arg = node.args(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == nullptr) {
      std::cerr << "Unable to create argument for boolean and\n";
      return nullptr;
    }
    children.push_back(n);
  }
  return TreeExprBuilder::MakeAnd(children);
}

NodePtr ProtoTypeToOrNode(const types::OrNode& node) {
  NodeVector children;

  for (int i = 0; i < node.args_size(); i++) {
    const types::TreeNode& arg = node.args(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == nullptr) {
      std::cerr << "Unable to create argument for boolean or\n";
      return nullptr;
    }
    children.push_back(n);
  }
  return TreeExprBuilder::MakeOr(children);
}

NodePtr ProtoTypeToNullNode(const types::NullNode& node) {
  DataTypePtr data_type = ProtoTypeToDataType(node.type());
  if (data_type == nullptr) {
    std::cerr << "Unknown type " << data_type->ToString() << " for null node\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeNull(data_type);
}

NodePtr ProtoTypeToNode(const types::TreeNode& node) {
  if (node.has_fieldnode()) {
    return ProtoTypeToFieldNode(node.fieldnode());
  }

  if (node.has_fnnode()) {
    return ProtoTypeToFnNode(node.fnnode());
  }

  if (node.has_ifnode()) {
    return ProtoTypeToIfNode(node.ifnode());
  }

  if (node.has_andnode()) {
    return ProtoTypeToAndNode(node.andnode());
  }

  if (node.has_ornode()) {
    return ProtoTypeToOrNode(node.ornode());
  }

  if (node.has_nullnode()) {
    return ProtoTypeToNullNode(node.nullnode());
  }

  if (node.has_intnode()) {
    return TreeExprBuilder::MakeLiteral(node.intnode().value());
  }

  if (node.has_floatnode()) {
    return TreeExprBuilder::MakeLiteral(node.floatnode().value());
  }

  if (node.has_longnode()) {
    return TreeExprBuilder::MakeLiteral(node.longnode().value());
  }

  if (node.has_booleannode()) {
    return TreeExprBuilder::MakeLiteral(node.booleannode().value());
  }

  if (node.has_doublenode()) {
    return TreeExprBuilder::MakeLiteral(node.doublenode().value());
  }

  if (node.has_stringnode()) {
    return TreeExprBuilder::MakeStringLiteral(node.stringnode().value());
  }

  if (node.has_binarynode()) {
    return TreeExprBuilder::MakeBinaryLiteral(node.binarynode().value());
  }

  std::cerr << "Unknown node type in protobuf\n";
  return nullptr;
}

ExpressionPtr ProtoTypeToExpression(const types::ExpressionRoot& root) {
  NodePtr root_node = ProtoTypeToNode(root.root());
  if (root_node == nullptr) {
    std::cerr << "Unable to create expression node from expression protobuf\n";
    return nullptr;
  }

  FieldPtr field = ProtoTypeToField(root.resulttype());
  if (field == nullptr) {
    std::cerr << "Unable to extra return field from expression protobuf\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeExpression(root_node, field);
}

ConditionPtr ProtoTypeToCondition(const types::Condition& condition) {
  NodePtr root_node = ProtoTypeToNode(condition.root());
  if (root_node == nullptr) {
    return nullptr;
  }

  return TreeExprBuilder::MakeCondition(root_node);
}

SchemaPtr ProtoTypeToSchema(const types::Schema& schema) {
  std::vector<FieldPtr> fields;

  for (int i = 0; i < schema.columns_size(); i++) {
    FieldPtr field = ProtoTypeToField(schema.columns(i));
    if (field == nullptr) {
      std::cerr << "Unable to extract arrow field from schema\n";
      return nullptr;
    }

    fields.push_back(field);
  }

  return arrow::schema(fields);
}

// Common for both projector and filters.

bool ParseProtobuf(uint8_t* buf, int bufLen, google::protobuf::Message* msg) {
  google::protobuf::io::CodedInputStream cis(buf, bufLen);
  cis.SetRecursionLimit(1000);
  return msg->ParseFromCodedStream(&cis);
}

Status make_record_batch_with_buf_addrs(SchemaPtr schema, int num_rows,
                                        jlong* in_buf_addrs, jlong* in_buf_sizes,
                                        int in_bufs_len,
                                        std::shared_ptr<arrow::RecordBatch>* batch) {
  std::vector<std::shared_ptr<arrow::ArrayData>> columns;
  auto num_fields = schema->num_fields();
  int buf_idx = 0;
  int sz_idx = 0;

  for (int i = 0; i < num_fields; i++) {
    auto field = schema->field(i);
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;

    if (buf_idx >= in_bufs_len) {
      return Status::Invalid("insufficient number of in_buf_addrs");
    }
    jlong validity_addr = in_buf_addrs[buf_idx++];
    jlong validity_size = in_buf_sizes[sz_idx++];
    auto validity = std::shared_ptr<arrow::Buffer>(
        new arrow::Buffer(reinterpret_cast<uint8_t*>(validity_addr), validity_size));
    buffers.push_back(validity);

    if (buf_idx >= in_bufs_len) {
      return Status::Invalid("insufficient number of in_buf_addrs");
    }
    jlong value_addr = in_buf_addrs[buf_idx++];
    jlong value_size = in_buf_sizes[sz_idx++];
    auto data = std::shared_ptr<arrow::Buffer>(
        new arrow::Buffer(reinterpret_cast<uint8_t*>(value_addr), value_size));
    buffers.push_back(data);

    if (arrow::is_binary_like(field->type()->id())) {
      if (buf_idx >= in_bufs_len) {
        return Status::Invalid("insufficient number of in_buf_addrs");
      }

      // add offsets buffer for variable-len fields.
      jlong offsets_addr = in_buf_addrs[buf_idx++];
      jlong offsets_size = in_buf_sizes[sz_idx++];
      auto offsets = std::shared_ptr<arrow::Buffer>(
          new arrow::Buffer(reinterpret_cast<uint8_t*>(offsets_addr), offsets_size));
      buffers.push_back(offsets);
    }

    auto array_data = arrow::ArrayData::Make(field->type(), num_rows, std::move(buffers));
    columns.push_back(array_data);
  }
  *batch = arrow::RecordBatch::Make(schema, num_rows, columns);
  return Status::OK();
}

// projector related functions.
void releaseProjectorInput(jbyteArray schema_arr, jbyte* schema_bytes,
                           jbyteArray exprs_arr, jbyte* exprs_bytes, JNIEnv* env) {
  env->ReleaseByteArrayElements(schema_arr, schema_bytes, JNI_ABORT);
  env->ReleaseByteArrayElements(exprs_arr, exprs_bytes, JNI_ABORT);
}

JNIEXPORT jlong JNICALL Java_org_apache_arrow_gandiva_evaluator_JniWrapper_buildProjector(
    JNIEnv* env, jobject obj, jbyteArray schema_arr, jbyteArray exprs_arr,
    jlong configuration_id) {
  jlong module_id = 0LL;
  std::shared_ptr<Projector> projector;
  std::shared_ptr<ProjectorHolder> holder;

  types::Schema schema;
  jsize schema_len = env->GetArrayLength(schema_arr);
  jbyte* schema_bytes = env->GetByteArrayElements(schema_arr, 0);

  types::ExpressionList exprs;
  jsize exprs_len = env->GetArrayLength(exprs_arr);
  jbyte* exprs_bytes = env->GetByteArrayElements(exprs_arr, 0);

  ExpressionVector expr_vector;
  SchemaPtr schema_ptr;
  FieldVector ret_types;
  gandiva::Status status;

  std::shared_ptr<Configuration> config = ConfigHolder::MapLookup(configuration_id);
  std::stringstream ss;

  if (config == nullptr) {
    ss << "configuration is mandatory.";
    releaseProjectorInput(schema_arr, schema_bytes, exprs_arr, exprs_bytes, env);
    goto err_out;
  }

  if (!ParseProtobuf(reinterpret_cast<uint8_t*>(schema_bytes), schema_len, &schema)) {
    ss << "Unable to parse schema protobuf\n";
    releaseProjectorInput(schema_arr, schema_bytes, exprs_arr, exprs_bytes, env);
    goto err_out;
  }

  if (!ParseProtobuf(reinterpret_cast<uint8_t*>(exprs_bytes), exprs_len, &exprs)) {
    releaseProjectorInput(schema_arr, schema_bytes, exprs_arr, exprs_bytes, env);
    ss << "Unable to parse expressions protobuf\n";
    goto err_out;
  }

  // convert types::Schema to arrow::Schema
  schema_ptr = ProtoTypeToSchema(schema);
  if (schema_ptr == nullptr) {
    ss << "Unable to construct arrow schema object from schema protobuf\n";
    releaseProjectorInput(schema_arr, schema_bytes, exprs_arr, exprs_bytes, env);
    goto err_out;
  }

  // create Expression out of the list of exprs
  for (int i = 0; i < exprs.exprs_size(); i++) {
    ExpressionPtr root = ProtoTypeToExpression(exprs.exprs(i));

    if (root == nullptr) {
      ss << "Unable to construct expression object from expression protobuf\n";
      releaseProjectorInput(schema_arr, schema_bytes, exprs_arr, exprs_bytes, env);
      goto err_out;
    }

    expr_vector.push_back(root);
    ret_types.push_back(root->result());
  }

  // good to invoke the evaluator now
  status = Projector::Make(schema_ptr, expr_vector, config, &projector);

  if (!status.ok()) {
    ss << "Failed to make LLVM module due to " << status.message() << "\n";
    releaseProjectorInput(schema_arr, schema_bytes, exprs_arr, exprs_bytes, env);
    goto err_out;
  }

  // store the result in a map
  holder = std::shared_ptr<ProjectorHolder>(
      new ProjectorHolder(schema_ptr, ret_types, std::move(projector)));
  module_id = projector_modules_.Insert(holder);
  releaseProjectorInput(schema_arr, schema_bytes, exprs_arr, exprs_bytes, env);
  return module_id;

err_out:
  env->ThrowNew(gandiva_exception_, ss.str().c_str());
  return module_id;
}

#define CHECK_OUT_BUFFER_IDX_AND_BREAK(idx, len)                               \
  if (idx >= len) {                                                            \
    status = gandiva::Status::Invalid("insufficient number of out_buf_addrs"); \
    break;                                                                     \
  }

JNIEXPORT void JNICALL
Java_org_apache_arrow_gandiva_evaluator_JniWrapper_evaluateProjector(
    JNIEnv* env, jobject cls, jlong module_id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jlongArray out_buf_addrs, jlongArray out_buf_sizes) {
  Status status;
  std::shared_ptr<ProjectorHolder> holder = projector_modules_.Lookup(module_id);
  if (holder == nullptr) {
    std::stringstream ss;
    ss << "Unknown module id " << module_id;
    env->ThrowNew(gandiva_exception_, ss.str().c_str());
    return;
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    env->ThrowNew(gandiva_exception_, "mismatch in arraylen of buf_addrs and buf_sizes");
    return;
  }

  int out_bufs_len = env->GetArrayLength(out_buf_addrs);
  if (out_bufs_len != env->GetArrayLength(out_buf_sizes)) {
    env->ThrowNew(gandiva_exception_,
                  "mismatch in arraylen of out_buf_addrs and out_buf_sizes");
    return;
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  jlong* out_bufs = env->GetLongArrayElements(out_buf_addrs, 0);
  jlong* out_sizes = env->GetLongArrayElements(out_buf_sizes, 0);

  do {
    std::shared_ptr<arrow::RecordBatch> in_batch;
    status = make_record_batch_with_buf_addrs(holder->schema(), num_rows, in_buf_addrs,
                                              in_buf_sizes, in_bufs_len, &in_batch);
    if (!status.ok()) {
      break;
    }

    auto ret_types = holder->rettypes();
    ArrayDataVector output;
    int buf_idx = 0;
    int sz_idx = 0;
    for (FieldPtr field : ret_types) {
      CHECK_OUT_BUFFER_IDX_AND_BREAK(buf_idx, out_bufs_len);
      uint8_t* validity_buf = reinterpret_cast<uint8_t*>(out_bufs[buf_idx++]);
      jlong bitmap_sz = out_sizes[sz_idx++];
      std::shared_ptr<arrow::MutableBuffer> bitmap_buf =
          std::make_shared<arrow::MutableBuffer>(validity_buf, bitmap_sz);

      CHECK_OUT_BUFFER_IDX_AND_BREAK(buf_idx, out_bufs_len);
      uint8_t* value_buf = reinterpret_cast<uint8_t*>(out_bufs[buf_idx++]);
      jlong data_sz = out_sizes[sz_idx++];
      std::shared_ptr<arrow::MutableBuffer> data_buf =
          std::make_shared<arrow::MutableBuffer>(value_buf, data_sz);

      auto array_data =
          arrow::ArrayData::Make(field->type(), num_rows, {bitmap_buf, data_buf});
      output.push_back(array_data);
    }
    if (!status.ok()) {
      break;
    }

    status = holder->projector()->Evaluate(*in_batch, output);
  } while (0);

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
  env->ReleaseLongArrayElements(out_buf_addrs, out_bufs, JNI_ABORT);
  env->ReleaseLongArrayElements(out_buf_sizes, out_sizes, JNI_ABORT);

  if (!status.ok()) {
    std::stringstream ss;
    ss << "Evaluate returned " << status.message() << "\n";
    env->ThrowNew(gandiva_exception_, status.message().c_str());
    return;
  }
}

JNIEXPORT void JNICALL Java_org_apache_arrow_gandiva_evaluator_JniWrapper_closeProjector(
    JNIEnv* env, jobject cls, jlong module_id) {
  projector_modules_.Erase(module_id);
}

// filter related functions.
void releaseFilterInput(jbyteArray schema_arr, jbyte* schema_bytes,
                        jbyteArray condition_arr, jbyte* condition_bytes, JNIEnv* env) {
  env->ReleaseByteArrayElements(schema_arr, schema_bytes, JNI_ABORT);
  env->ReleaseByteArrayElements(condition_arr, condition_bytes, JNI_ABORT);
}

JNIEXPORT jlong JNICALL Java_org_apache_arrow_gandiva_evaluator_JniWrapper_buildFilter(
    JNIEnv* env, jobject obj, jbyteArray schema_arr, jbyteArray condition_arr,
    jlong configuration_id) {
  jlong module_id = 0LL;
  std::shared_ptr<Filter> filter;
  std::shared_ptr<FilterHolder> holder;

  types::Schema schema;
  jsize schema_len = env->GetArrayLength(schema_arr);
  jbyte* schema_bytes = env->GetByteArrayElements(schema_arr, 0);

  types::Condition condition;
  jsize condition_len = env->GetArrayLength(condition_arr);
  jbyte* condition_bytes = env->GetByteArrayElements(condition_arr, 0);

  ConditionPtr condition_ptr;
  SchemaPtr schema_ptr;
  gandiva::Status status;

  std::shared_ptr<Configuration> config = ConfigHolder::MapLookup(configuration_id);
  std::stringstream ss;

  if (config == nullptr) {
    ss << "configuration is mandatory.";
    releaseFilterInput(schema_arr, schema_bytes, condition_arr, condition_bytes, env);
    goto err_out;
  }

  if (!ParseProtobuf(reinterpret_cast<uint8_t*>(schema_bytes), schema_len, &schema)) {
    ss << "Unable to parse schema protobuf\n";
    releaseFilterInput(schema_arr, schema_bytes, condition_arr, condition_bytes, env);
    goto err_out;
  }

  if (!ParseProtobuf(reinterpret_cast<uint8_t*>(condition_bytes), condition_len,
                     &condition)) {
    ss << "Unable to parse condition protobuf\n";
    releaseFilterInput(schema_arr, schema_bytes, condition_arr, condition_bytes, env);
    goto err_out;
  }

  // convert types::Schema to arrow::Schema
  schema_ptr = ProtoTypeToSchema(schema);
  if (schema_ptr == nullptr) {
    ss << "Unable to construct arrow schema object from schema protobuf\n";
    releaseFilterInput(schema_arr, schema_bytes, condition_arr, condition_bytes, env);
    goto err_out;
  }

  condition_ptr = ProtoTypeToCondition(condition);
  if (condition_ptr == nullptr) {
    ss << "Unable to construct condition object from condition protobuf\n";
    releaseFilterInput(schema_arr, schema_bytes, condition_arr, condition_bytes, env);
    goto err_out;
  }

  // good to invoke the filter builder now
  status = Filter::Make(schema_ptr, condition_ptr, config, &filter);
  if (!status.ok()) {
    ss << "Failed to make LLVM module due to " << status.message() << "\n";
    releaseFilterInput(schema_arr, schema_bytes, condition_arr, condition_bytes, env);
    goto err_out;
  }

  // store the result in a map
  holder = std::shared_ptr<FilterHolder>(new FilterHolder(schema_ptr, std::move(filter)));
  module_id = filter_modules_.Insert(holder);
  releaseFilterInput(schema_arr, schema_bytes, condition_arr, condition_bytes, env);
  return module_id;

err_out:
  env->ThrowNew(gandiva_exception_, ss.str().c_str());
  return module_id;
}

JNIEXPORT jint JNICALL Java_org_apache_arrow_gandiva_evaluator_JniWrapper_evaluateFilter(
    JNIEnv* env, jobject cls, jlong module_id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jint jselection_vector_type, jlong out_buf_addr,
    jlong out_buf_size) {
  gandiva::Status status;
  std::shared_ptr<FilterHolder> holder = filter_modules_.Lookup(module_id);
  if (holder == nullptr) {
    env->ThrowNew(gandiva_exception_, "Unknown module id\n");
    return -1;
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    env->ThrowNew(gandiva_exception_, "mismatch in arraylen of buf_addrs and buf_sizes");
    return -1;
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);
  std::shared_ptr<gandiva::SelectionVector> selection_vector;

  do {
    std::shared_ptr<arrow::RecordBatch> in_batch;

    status = make_record_batch_with_buf_addrs(holder->schema(), num_rows, in_buf_addrs,
                                              in_buf_sizes, in_bufs_len, &in_batch);
    if (!status.ok()) {
      break;
    }

    auto selection_vector_type =
        static_cast<types::SelectionVectorType>(jselection_vector_type);
    auto out_buffer = std::make_shared<arrow::MutableBuffer>(
        reinterpret_cast<uint8_t*>(out_buf_addr), out_buf_size);
    switch (selection_vector_type) {
      case types::SV_INT16:
        status =
            gandiva::SelectionVector::MakeInt16(num_rows, out_buffer, &selection_vector);
        break;
      case types::SV_INT32:
        status =
            gandiva::SelectionVector::MakeInt32(num_rows, out_buffer, &selection_vector);
        break;
      default:
        status = gandiva::Status::Invalid("unknown selection vector type");
    }
    if (!status.ok()) {
      break;
    }

    status = holder->filter()->Evaluate(*in_batch, selection_vector);
  } while (0);

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  if (!status.ok()) {
    std::stringstream ss;
    ss << "Evaluate returned " << status.message() << "\n";
    env->ThrowNew(gandiva_exception_, status.message().c_str());
    return -1;
  } else {
    return selection_vector->GetNumSlots();
  }
}

JNIEXPORT void JNICALL Java_org_apache_arrow_gandiva_evaluator_JniWrapper_closeFilter(
    JNIEnv* env, jobject cls, jlong module_id) {
  filter_modules_.Erase(module_id);
}

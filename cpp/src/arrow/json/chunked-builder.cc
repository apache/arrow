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

#include "arrow/json/chunked-builder.h"

#include <mutex>
#include <unordered_map>

#include "arrow/builder.h"
#include "arrow/table.h"
#include "arrow/util/stl.h"

namespace arrow {
namespace json {

class TypedChunkedArrayBuilder : public ChunkedArrayBuilder {
 public:
  TypedChunkedArrayBuilder(const std::shared_ptr<internal::TaskGroup>& task_group,
                           std::shared_ptr<Converter> converter)
      : ChunkedArrayBuilder(task_group), converter_(std::move(converter)) {}

  void Insert(int64_t block_index, const std::shared_ptr<Field>&,
              const std::shared_ptr<Array>& unconverted) override {
    std::unique_lock<std::mutex> lock(mutex_);
    if (chunks_.size() <= static_cast<size_t>(block_index)) {
      chunks_.resize(static_cast<size_t>(block_index) + 1);
    }
    lock.unlock();

    task_group_->Append([this, block_index, unconverted] {
      std::shared_ptr<Array> converted;
      RETURN_NOT_OK(converter_->Convert(unconverted, &converted));
      std::unique_lock<std::mutex> lock(mutex_);
      chunks_[block_index] = std::move(converted);
      return Status::OK();
    });
  }

  Status Finish(std::shared_ptr<ChunkedArray>* out) override {
    RETURN_NOT_OK(task_group_->Finish());
    *out = std::make_shared<ChunkedArray>(chunks_, converter_->out_type());
    chunks_.clear();
    return Status::OK();
  }

 private:
  ArrayVector chunks_;
  std::mutex mutex_;
  std::shared_ptr<Converter> converter_;
};

class InferringChunkedArrayBuilder : public ChunkedArrayBuilder {
 public:
  InferringChunkedArrayBuilder(const std::shared_ptr<internal::TaskGroup>& task_group,
                               const PromotionGraph* promotion_graph,
                               std::shared_ptr<Converter> converter)
      : ChunkedArrayBuilder(task_group),
        promotion_graph_(promotion_graph),
        converter_(std::move(converter)) {}

  void Insert(int64_t block_index, const std::shared_ptr<Field>& unconverted_field,
              const std::shared_ptr<Array>& unconverted) override {
    std::unique_lock<std::mutex> lock(mutex_);
    if (chunks_.size() <= static_cast<size_t>(block_index)) {
      chunks_.resize(static_cast<size_t>(block_index) + 1);
      unconverted_.resize(chunks_.size());
    }
    unconverted_[block_index] = unconverted;
    lock.unlock();
    ScheduleConvertChunk(block_index, unconverted_field);
  }

  void ScheduleConvertChunk(int64_t block_index,
                            const std::shared_ptr<Field>& unconverted_field) {
    task_group_->Append([this, block_index, unconverted_field] {
      return TryConvertChunk(static_cast<size_t>(block_index), unconverted_field);
    });
  }

  Status TryConvertChunk(size_t block_index,
                         const std::shared_ptr<Field>& unconverted_field) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto converter = converter_;
    auto unconverted = unconverted_[block_index];
    std::shared_ptr<Array> converted;

    lock.unlock();
    Status st = converter_->Convert(unconverted, &converted);
    lock.lock();

    if (converter != converter_) {
      // another task promoted converter; reconvert
      lock.unlock();
      ScheduleConvertChunk(block_index, unconverted_field);
      return Status::OK();
    }

    if (st.ok()) {
      // conversion succeeded
      chunks_[block_index] = std::move(converted);
      unconverted_[block_index] = unconverted;
      return Status::OK();
    }

    auto promoted_type =
        promotion_graph_->Promote(converter_->out_type(), unconverted_field);
    if (promoted_type == nullptr) {
      // converter failed, no promotion available
      return st;
    }
    RETURN_NOT_OK(MakeConverter(promoted_type, converter_->pool(), &converter_));

    size_t nchunks = chunks_.size();
    for (size_t i = 0; i < nchunks; ++i) {
      if (i != block_index && chunks_[i]) {
        // We're assuming the chunk was converted using the wrong type
        // (which should be true unless the executor reorders tasks)
        chunks_[i].reset();
        lock.unlock();
        ScheduleConvertChunk(i, unconverted_field);
        lock.lock();
      }
    }
    lock.unlock();
    ScheduleConvertChunk(block_index, unconverted_field);
    return Status::OK();
  }

  Status Finish(std::shared_ptr<ChunkedArray>* out) override {
    RETURN_NOT_OK(task_group_->Finish());
    *out = std::make_shared<ChunkedArray>(chunks_, converter_->out_type());
    chunks_.clear();
    unconverted_.clear();
    return Status::OK();
  }

 private:
  ArrayVector chunks_, unconverted_;
  std::mutex mutex_;
  const PromotionGraph* promotion_graph_;
  std::shared_ptr<Converter> converter_;
};

class ChunkedListArrayBuilder : public ChunkedArrayBuilder {
 public:
  ChunkedListArrayBuilder(const std::shared_ptr<internal::TaskGroup>& task_group,
                          std::unique_ptr<ChunkedArrayBuilder> value_builder,
                          util::string_view field_name)
      : ChunkedArrayBuilder(task_group),
        value_builder_(std::move(value_builder)),
        field_name_(field_name) {}

  void Insert(int64_t block_index, const std::shared_ptr<Field>&,
              const std::shared_ptr<Array>& unconverted) override {
    auto list_array = static_cast<const ListArray*>(unconverted.get());
    value_builder_->Insert(block_index, list_array->list_type()->value_field(),
                           list_array->values());

    std::unique_lock<std::mutex> lock(mutex_);
    if (null_bitmap_chunks_.size() <= static_cast<size_t>(block_index)) {
      null_bitmap_chunks_.resize(static_cast<size_t>(block_index) + 1);
      offset_chunks_.resize(null_bitmap_chunks_.size());
    }
    null_bitmap_chunks_[block_index] = unconverted->null_bitmap();
    offset_chunks_[block_index] = list_array->value_offsets();
  }

  Status Finish(std::shared_ptr<ChunkedArray>* out) override {
    RETURN_NOT_OK(task_group_->Finish());

    std::shared_ptr<ChunkedArray> child_array;
    RETURN_NOT_OK(value_builder_->Finish(&child_array));

    auto type = list(field(field_name_, child_array->type()));
    ArrayVector chunks(null_bitmap_chunks_.size());
    for (size_t i = 0; i < null_bitmap_chunks_.size(); ++i) {
      auto child_chunk = child_array->chunk(i);
      chunks[i] =
          std::make_shared<ListArray>(type, child_chunk->length(), offset_chunks_[i],
                                      child_chunk, null_bitmap_chunks_[i]);
    }

    *out = std::make_shared<ChunkedArray>(std::move(chunks), type);
    return Status::OK();
  }

 private:
  std::mutex mutex_;
  std::unique_ptr<ChunkedArrayBuilder> value_builder_;
  BufferVector offset_chunks_, null_bitmap_chunks_;
  std::string field_name_;
};

class ChunkedStructArrayBuilder : public ChunkedArrayBuilder {
 public:
  ChunkedStructArrayBuilder(
      const std::shared_ptr<internal::TaskGroup>& task_group, MemoryPool* pool,
      const PromotionGraph* promotion_graph,
      std::vector<std::pair<std::string, std::unique_ptr<ChunkedArrayBuilder>>>
          name_builders)
      : ChunkedArrayBuilder(task_group), pool_(pool), promotion_graph_(promotion_graph) {
    for (auto&& name_builder : name_builders) {
      name_to_index_.emplace(std::move(name_builder.first), name_to_index_.size());
      child_builders_.emplace_back(std::move(name_builder.second));
    }
  }

  void Insert(int64_t block_index, const std::shared_ptr<Field>&,
              const std::shared_ptr<Array>& unconverted) override {
    auto struct_array = std::static_pointer_cast<StructArray>(unconverted);
    if (promotion_graph_ == nullptr) {
      // If unexpected fields are ignored or result in an error then all parsers will emit
      // columns exclusively in the ordering specified in ParseOptions::explicit_schema,
      // so child_builders_ is immutable and no associative lookup is necessary.
      for (int i = 0; i < unconverted->num_fields(); ++i) {
        child_builders_[i]->Insert(block_index, unconverted->type()->child(i),
                                   struct_array->field(i));
      }
    } else {
      task_group_->Append([this, block_index, struct_array] {
        return InsertChildren(block_index, struct_array.get());
      });
    }

    std::unique_lock<std::mutex> lock(null_mutex_);
    if (null_bitmap_chunks_.size() <= static_cast<size_t>(block_index)) {
      null_bitmap_chunks_.resize(static_cast<size_t>(block_index) + 1);
      chunk_lengths_.resize(null_bitmap_chunks_.size());
    }
    null_bitmap_chunks_[block_index] = unconverted->null_bitmap();
    chunk_lengths_[block_index] = unconverted->length();
  }

  // Insert children associatively by name; the unconverted block may have unexpected or
  // differently ordered fields
  Status InsertChildren(int64_t block_index, const StructArray* unconverted) {
    std::unique_lock<std::mutex> lock(children_mutex_);
    const auto& fields = unconverted->type()->children();

    for (int i = 0; i < unconverted->num_fields(); ++i) {
      auto it = name_to_index_.find(fields[i]->name());

      if (it == name_to_index_.end()) {
        // add a new field to this builder
        auto type = promotion_graph_->Infer(fields[i]);
        DCHECK_NE(type, nullptr)
            << "invalid unconverted_field encountered in conversion: "
            << fields[i]->name() << ":" << *fields[i]->type();

        it = name_to_index_.emplace(fields[i]->name(), name_to_index_.size()).first;

        std::unique_ptr<ChunkedArrayBuilder> child_builder;
        RETURN_NOT_OK(MakeChunkedArrayBuilder(task_group_, pool_, promotion_graph_, type,
                                              &child_builder));
        child_builders_.emplace_back(std::move(child_builder));
      }

      auto unconverted_field = unconverted->type()->child(i);
      child_builders_[it->second]->Insert(block_index, unconverted_field,
                                          unconverted->field(i));
    }

    return Status::OK();
  }

  Status Finish(std::shared_ptr<ChunkedArray>* out) override {
    RETURN_NOT_OK(task_group_->Finish());

    std::vector<std::shared_ptr<Field>> fields(name_to_index_.size());
    std::vector<std::shared_ptr<ChunkedArray>> child_arrays(name_to_index_.size());
    for (auto&& name_index : name_to_index_) {
      auto child_builder = child_builders_[name_index.second].get();

      std::shared_ptr<ChunkedArray> child_array;
      RETURN_NOT_OK(child_builder->Finish(&child_array));

      child_arrays[name_index.second] = child_array;
      fields[name_index.second] = field(name_index.first, child_array->type());
    }

    auto type = struct_(std::move(fields));
    ArrayVector chunks(null_bitmap_chunks_.size());
    for (size_t i = 0; i < null_bitmap_chunks_.size(); ++i) {
      ArrayVector child_chunks;
      for (const auto& child_array : child_arrays) {
        child_chunks.push_back(child_array->chunk(i));
      }
      chunks[i] = std::make_shared<StructArray>(type, chunk_lengths_[i], child_chunks,
                                                null_bitmap_chunks_[i]);
    }

    *out = std::make_shared<ChunkedArray>(std::move(chunks), type);
    return Status::OK();
  }

 private:
  std::mutex null_mutex_, children_mutex_;
  MemoryPool* pool_;
  const PromotionGraph* promotion_graph_;
  std::unordered_map<std::string, int> name_to_index_;
  std::vector<std::unique_ptr<ChunkedArrayBuilder>> child_builders_;
  BufferVector null_bitmap_chunks_;
  std::vector<int64_t> chunk_lengths_;
};

Status MakeChunkedArrayBuilder(const std::shared_ptr<internal::TaskGroup>& task_group,
                               MemoryPool* pool, const PromotionGraph* promotion_graph,
                               const std::shared_ptr<DataType>& type,
                               std::unique_ptr<ChunkedArrayBuilder>* out) {
  if (type->id() == Type::STRUCT) {
    std::vector<std::pair<std::string, std::unique_ptr<ChunkedArrayBuilder>>>
        child_builders;
    for (const auto& f : type->children()) {
      std::unique_ptr<ChunkedArrayBuilder> child_builder;
      RETURN_NOT_OK(MakeChunkedArrayBuilder(task_group, pool, promotion_graph, f->type(),
                                            &child_builder));
      child_builders.emplace_back(f->name(), std::move(child_builder));
    }
    *out = internal::make_unique<ChunkedStructArrayBuilder>(
        task_group, pool, promotion_graph, std::move(child_builders));
    return Status::OK();
  }
  if (type->id() == Type::LIST) {
    auto list_type = static_cast<const ListType*>(type.get());
    std::unique_ptr<ChunkedArrayBuilder> value_builder;
    RETURN_NOT_OK(MakeChunkedArrayBuilder(task_group, pool, promotion_graph,
                                          list_type->value_type(), &value_builder));
    *out = internal::make_unique<ChunkedListArrayBuilder>(
        task_group, std::move(value_builder), list_type->value_field()->name());
    return Status::OK();
  }
  std::shared_ptr<Converter> converter;
  RETURN_NOT_OK(MakeConverter(type, pool, &converter));
  if (promotion_graph) {
    *out = internal::make_unique<InferringChunkedArrayBuilder>(
        task_group, promotion_graph, std::move(converter));
  } else {
    *out =
        internal::make_unique<TypedChunkedArrayBuilder>(task_group, std::move(converter));
  }
  return Status::OK();
}

}  // namespace json
}  // namespace arrow

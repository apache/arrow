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

#include "spilling_util.h"
#include <mutex>

#ifdef _WIN32
// "windows_compatibility.h" includes <windows.h>, which must go BEFORE
// <fileapi.h> because <windows.h> defines some architecture stuff that <fileapi.h>
// needs.
// clang-format off
#include "arrow/util/windows_compatibility.h"
#include "arrow/util/io_util.h"
#include <fileapi.h>
// clang-format on
#endif

namespace arrow {
namespace compute {
struct ArrayInfo {
  int64_t num_children;
  int64_t length;
  int64_t null_count;
  std::shared_ptr<DataType> type;
  std::array<std::shared_ptr<Buffer>, 3> bufs;
  std::array<size_t, 3> sizes;
  std::shared_ptr<ArrayData> dictionary;
};

#ifdef _WIN32

struct SpillFile::BatchInfo {
  int64_t start;  // Offset of batch in file
  std::vector<ArrayInfo> arrays;
};

const FileHandle kInvalidHandle = INVALID_HANDLE_VALUE;

static Result<FileHandle> OpenTemporaryFile() {
  constexpr DWORD kTempFileNameSize = MAX_PATH + 1;
  wchar_t tmp_name_buf[kTempFileNameSize];
  wchar_t tmp_path_buf[kTempFileNameSize];

  DWORD ret;
  ret = GetTempPathW(kTempFileNameSize, tmp_path_buf);
  if (ret > kTempFileNameSize || ret == 0)
    return arrow::internal::IOErrorFromWinError(GetLastError(),
                                                "Failed to get temporary file path");
  if (GetTempFileNameW(tmp_path_buf, L"ARROW_TMP", 0, tmp_name_buf) == 0)
    return arrow::internal::IOErrorFromWinError(GetLastError(),
                                                "Failed to get temporary file name");

  HANDLE file_handle = CreateFileW(
      tmp_name_buf, GENERIC_READ | GENERIC_WRITE | FILE_APPEND_DATA, 0, NULL,
      CREATE_ALWAYS,
      FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED | FILE_FLAG_DELETE_ON_CLOSE, NULL);
  if (file_handle == INVALID_HANDLE_VALUE)
    return arrow::internal::IOErrorFromWinError(GetLastError(),
                                                "Failed to create temp file");
  return file_handle;
}

static Status CloseTemporaryFile(FileHandle* handle) {
  if (!CloseHandle(*handle)) return Status::IOError("Failed to close temp file");
  *handle = kInvalidHandle;
  return Status::OK();
}

static Status WriteBatch_PlatformSpecific(FileHandle handle, SpillFile::BatchInfo& info) {
  OVERLAPPED overlapped;
  int64_t offset = info.start;
  for (ArrayInfo& arr : info.arrays) {
    for (size_t i = 0; i < arr.bufs.size(); i++) {
      if (arr.bufs[i] != 0) {
        overlapped.Offset = static_cast<DWORD>(offset & ~static_cast<DWORD>(0));
        overlapped.OffsetHigh =
            static_cast<DWORD>((offset >> 32) & ~static_cast<DWORD>(0));
        if (!WriteFile(handle, arr.bufs[i]->data(), static_cast<DWORD>(arr.sizes[i]),
                       NULL, &overlapped))
          return arrow::internal::IOErrorFromWinError(
              GetLastError(), "Failed to write to temporary file");

        offset += arr.sizes[i];
        arr.bufs[i].reset();
      }
    }
  }
  return Status::OK();
}

static Result<std::shared_ptr<ArrayData>> ReconstructArray(const FileHandle handle,
                                                           size_t& idx,
                                                           std::vector<ArrayInfo>& arrs,
                                                           size_t& current_offset) {
  ArrayInfo& arr = arrs[idx++];
  std::shared_ptr<ArrayData> data = std::make_shared<ArrayData>();
  data->type = std::move(arr.type);
  data->length = arr.length;
  data->null_count = arr.null_count;
  data->dictionary = std::move(arr.dictionary);

  data->buffers.resize(3);
  for (int i = 0; i < 3; i++) {
    if (arr.sizes[i]) {
      data->buffers[i] = std::move(arr.bufs[i]);

      OVERLAPPED overlapped;
      overlapped.Offset = static_cast<DWORD>(current_offset & static_cast<DWORD>(~0));
#ifdef _WIN64
      overlapped.OffsetHigh =
          static_cast<DWORD>((current_offset >> 32) & static_cast<DWORD>(~0));
#else
      overlapped.OffsetHigh = static_cast<DWORD>(0);
#endif
      if (!ReadFile(handle, static_cast<void*>(data->buffers[i]->mutable_data()),
                    static_cast<DWORD>(arr.sizes[i]), NULL, &overlapped))
        return Status::IOError("Failed to read back spilled data!");
      current_offset += arr.sizes[i];
    }
  }
  data->child_data.resize(arr.num_children);
  for (int i = 0; i < arr.num_children; i++) {
    ARROW_ASSIGN_OR_RAISE(data->child_data[i],
                          ReconstructArray(handle, idx, arrs, current_offset));
  }

  return data;
}

static Result<ExecBatch> ReadBatch_PlatformSpecific(FileHandle handle,
                                                    SpillFile::BatchInfo& info) {
  std::vector<Datum> batch;
  size_t offset = info.start;
  // ReconstructArray increments i
  for (size_t i = 0; i < info.arrays.size();) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> ad,
                          ReconstructArray(handle, i, info.arrays, offset));
    batch.emplace_back(std::move(ad));
  }
  return ExecBatch::Make(std::move(batch));
}

#else
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

struct SpillFile::BatchInfo {
  int64_t start;
  std::vector<ArrayInfo> arrays;
  std::vector<struct iovec> ios;
};

Result<FileHandle> OpenTemporaryFile() {
  static std::once_flag generate_tmp_file_name_flag;

  constexpr int kFileNameSize = 1024;
  static char name_template[kFileNameSize];
  char name[kFileNameSize];

  char* name_template_ptr = name_template;
  std::call_once(generate_tmp_file_name_flag, [name_template_ptr]() noexcept {
    const char* selectors[] = {"TMPDIR", "TMP", "TEMP", "TEMPDIR"};
    constexpr size_t kNumSelectors = sizeof(selectors) / sizeof(selectors[0]);
#ifdef __ANDROID__
    const char* backup = "/data/local/tmp/";
#else
        const char *backup = "/var/tmp/";
#endif
    const char* tmp_dir = backup;
    for (size_t i = 0; i < kNumSelectors; i++) {
      const char* env = getenv(selectors[i]);
      if (env) {
        tmp_dir = env;
        break;
      }
    }
    size_t tmp_dir_length = std::strlen(tmp_dir);

    const char* tmp_name_template = "/ARROW_TMP_XXXXXX";
    size_t tmp_name_length = std::strlen(tmp_name_template);

    if ((tmp_dir_length + tmp_name_length) >= kFileNameSize) {
      tmp_dir = backup;
      tmp_dir_length = std::strlen(backup);
    }

    std::strncpy(name_template_ptr, tmp_dir, kFileNameSize);
    std::strncpy(name_template_ptr + tmp_dir_length, tmp_name_template,
                 kFileNameSize - tmp_dir_length);
  });

  std::strncpy(name, name_template, kFileNameSize);

#ifdef __APPLE__
  int fd = mkstemp(name);
  if (fd == kInvalidHandle) return Status::IOError(strerror(errno));
  if (fcntl(fd, F_NOCACHE, 1) == -1) return Status::IOError(strerror(errno));
#else
  // If we failed, it's possible the temp directory didn't like O_DIRECT,
  // so we try again without O_DIRECT, and if it still doesn't work then
  // give up.
  int fd = mkostemp(name, O_DIRECT);
  if (fd == kInvalidHandle) {
    std::strncpy(name, name_template, kFileNameSize);
    fd = mkstemp(name);
    if (fd == kInvalidHandle) return Status::IOError(strerror(errno));
  }
#endif

  if (unlink(name) != 0) return Status::IOError(strerror(errno));
  return fd;
}

static Status CloseTemporaryFile(FileHandle* handle) {
  if (close(*handle) == -1) return Status::IOError(strerror(errno));
  *handle = kInvalidHandle;
  return Status::OK();
}

static Status WriteBatch_PlatformSpecific(FileHandle handle, SpillFile::BatchInfo& info) {
  if (pwritev(handle, info.ios.data(), static_cast<int>(info.ios.size()), info.start) ==
      -1)
    return Status::IOError("Failed to spill!");

  // Release all references to the buffers, freeing them.
  for (ArrayInfo& arr : info.arrays)
    for (int i = 0; i < 3; i++)
      if (arr.bufs[i]) arr.bufs[i].reset();
  return Status::OK();
}

static Result<std::shared_ptr<ArrayData>> ReconstructArray(size_t& idx,
                                                           SpillFile::BatchInfo& info) {
  ArrayInfo& arr = info.arrays[idx++];
  std::shared_ptr<ArrayData> data = std::make_shared<ArrayData>();
  data->type = std::move(arr.type);
  data->length = arr.length;
  data->null_count = arr.null_count;
  data->dictionary = std::move(arr.dictionary);
  data->buffers.resize(3);
  for (int i = 0; i < 3; i++)
    if (arr.sizes[i]) data->buffers[i] = std::move(arr.bufs[i]);

  data->child_data.resize(arr.num_children);
  for (int i = 0; i < arr.num_children; i++) {
    ARROW_ASSIGN_OR_RAISE(data->child_data[i], ReconstructArray(idx, info));
  }
  return data;
}

static Result<ExecBatch> ReadBatch_PlatformSpecific(FileHandle handle,
                                                    SpillFile::BatchInfo& info) {
  std::vector<Datum> batch;
  // ReconstructArray increments i
  for (size_t i = 0; i < info.arrays.size();) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> ad, ReconstructArray(i, info));
    batch.emplace_back(std::move(ad));
  }

  if (preadv(handle, info.ios.data(), static_cast<int>(info.ios.size()), info.start) ==
      -1)
    return Status::IOError(std::string("Failed to read back spilled data: ") +
                           std::strerror(errno));

  return ExecBatch::Make(std::move(batch));
}
#endif

SpillFile::~SpillFile() {
  Status st = Cleanup();
  if (!st.ok()) st.Warn();
}

static Status CollectArrayInfo(SpillFile::BatchInfo& batch_info, int64_t& total_size,
                               ArrayData* array) {
  if (array->offset != 0)
    return Status::Invalid("We don't support spilling arrays with offsets");

  batch_info.arrays.push_back({});
  ArrayInfo& array_info = batch_info.arrays.back();
  array_info.type = std::move(array->type);
  array_info.length = array->length;
  array_info.null_count = array->null_count.load(std::memory_order_relaxed);

  ARROW_DCHECK(array->buffers.size() <= array_info.bufs.size());
  array_info.num_children = array->child_data.size();
  for (size_t i = 0; i < array->buffers.size(); i++) {
    if (array->buffers[i]) {
      array_info.sizes[i] = array->buffers[i]->size();
      total_size += array_info.sizes[i];
      uintptr_t addr = array->buffers[i]->address();
      if ((addr % SpillFile::kAlignment) != 0)
        return Status::Invalid("Buffer not aligned to 512 bytes!");
      array_info.bufs[i] = std::move(array->buffers[i]);
#ifndef _WIN32
      struct iovec io;
      io.iov_base = static_cast<void*>(array_info.bufs[i]->mutable_data());
      io.iov_len = static_cast<size_t>(array_info.sizes[i]);
      batch_info.ios.push_back(io);
#endif
    } else {
      array_info.sizes[i] = 0;
    }
  }

  array_info.dictionary = std::move(array->dictionary);
  for (std::shared_ptr<ArrayData>& child : array->child_data)
    RETURN_NOT_OK(CollectArrayInfo(batch_info, total_size, child.get()));

  // Cleanup the ArrayData
  array->type.reset();
  array->length = 0;
  return Status::OK();
}

static Status AllocateBuffersForBatch(SpillFile::BatchInfo& batch_info,
                                      MemoryPool* pool) {
#ifndef _WIN32
  size_t iiovec = 0;
#endif
  for (ArrayInfo& arr : batch_info.arrays) {
    for (size_t ibuf = 0; ibuf < 3; ibuf++) {
      if (arr.sizes[ibuf]) {
        ARROW_ASSIGN_OR_RAISE(
            arr.bufs[ibuf], AllocateBuffer(arr.sizes[ibuf], SpillFile::kAlignment, pool));
#ifndef _WIN32
        batch_info.ios[iiovec].iov_base =
            static_cast<void*>(arr.bufs[ibuf]->mutable_data());
        batch_info.ios[iiovec].iov_len = static_cast<size_t>(arr.sizes[ibuf]);
        iiovec++;
#endif
      }
    }
  }
  return Status::OK();
}

Status SpillFile::SpillBatch(QueryContext* ctx, ExecBatch batch) {
  if (handle_ == kInvalidHandle) {
    ARROW_ASSIGN_OR_RAISE(handle_, OpenTemporaryFile());
  }
  int64_t total_size = 0;
  batches_.emplace_back(new BatchInfo);
  BatchInfo* info = batches_.back();
  for (int i = 0; i < batch.num_values(); i++) {
    if (batch[i].is_scalar()) return Status::Invalid("Cannot spill a Scalar");
    RETURN_NOT_OK(CollectArrayInfo(*info, total_size, batch[i].mutable_array()));
  }
  info->start = size_;
  size_ += total_size;

  FileHandle handle = handle_;
  RETURN_NOT_OK(ctx->ScheduleIOTask([this, handle, info, ctx, total_size]() {
    auto mark = ctx->ReportTempFileIO(total_size);
    RETURN_NOT_OK(WriteBatch_PlatformSpecific(handle, *info));
    if (++batches_written_ == batches_.size() && read_requested_.load()) {
      bool expected = false;
      if (read_started_.compare_exchange_strong(expected, true))
        return ctx->ScheduleTask([this, ctx]() { return ScheduleReadbackTasks(ctx); });
    }
    return Status::OK();
  }));
  return Status::OK();
}

Status SpillFile::ReadBackBatches(QueryContext* ctx,
                                  std::function<Status(size_t, size_t, ExecBatch)> fn,
                                  std::function<Status(size_t)> on_finished) {
  readback_fn_ = std::move(fn);
  on_readback_finished_ = std::move(on_finished);

  read_requested_.store(true);
  if (batches_written_ == batches_.size()) {
    bool expected = false;
    if (read_started_.compare_exchange_strong(expected, true))
      return ScheduleReadbackTasks(ctx);
  }
  return Status::OK();
}

Status SpillFile::Cleanup() {
  if (handle_ != kInvalidHandle) RETURN_NOT_OK(CloseTemporaryFile(&handle_));
  for (BatchInfo* b : batches_) delete b;

  batches_.clear();
  return Status::OK();
}

Status SpillFile::PreallocateBatches(MemoryPool* memory_pool) {
  preallocated_ = true;
  for (size_t i = 0; i < batches_.size(); i++) {
    RETURN_NOT_OK(AllocateBuffersForBatch(*batches_[i], memory_pool));
  }
  return Status::OK();
}

Status SpillFile::OnBatchRead(size_t thread_index, size_t batch_index, ExecBatch batch) {
  RETURN_NOT_OK(readback_fn_(thread_index, batch_index, std::move(batch)));
  if (++batches_read_ == batches_.size()) return on_readback_finished_(thread_index);
  return Status::OK();
}

Status SpillFile::ScheduleReadbackTasks(QueryContext* ctx) {
  if (batches_.empty()) return on_readback_finished_(ctx->GetThreadIndex());

  for (size_t i = 0; i < batches_.size(); i++) {
    BatchInfo* info = batches_[i];
    if (!preallocated_) RETURN_NOT_OK(AllocateBuffersForBatch(*info, ctx->memory_pool()));
    RETURN_NOT_OK(ctx->ScheduleIOTask([this, i, info, ctx]() {
      ARROW_ASSIGN_OR_RAISE(ExecBatch batch, ReadBatch_PlatformSpecific(handle_, *info));
      RETURN_NOT_OK(ctx->ScheduleTask(
          [this, i, batch = std::move(batch)](size_t thread_index) mutable {
            return OnBatchRead(thread_index, i, std::move(batch));
          }));
      return Status::OK();
    }));
  }
  return Status::OK();
}
}  // namespace compute
}  // namespace arrow

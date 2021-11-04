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

// shared memory data plane driver
// - client and server use fifo (named pipe) for messaging
// - client and server mmap one pre-allocated shared buffer for flight payload
//   exchanging, with best performance
// - if no shared buffer available, an one-shot buffer is created on demand for
//   each read/write operation, performance is probably poor

// TODO(yibo):
// - implement back pressure (common for all data planes)
// - replace fifo with unix socket for ipc
//   * current fifo based approach may leave garbage ipc files on crash
//     with unix socket: unlink ipc files immediately after open, pass fd
//   * fix a race issue (see ShmFifo:~ShmFifo())
//   * socket based messaging may be re-usable by other data planes
// - IS IT POSSIBLE to use existing grpc control path for data plane messaging
// - improve buffer cache management
//   * finer de-/allocation, better resource usage, drop one-shot buffer
//   * better to be re-usable for other data planes

// XXX: performance depends heavily on if the buffer cache is used effectively
// - if payload size > kBufferSize, cache cannot be used, performance suffers
// - cache capacity (kBufferCount) limits pending buffers not consumed by the
//   reader, performance may suffer if reader is slower than the writer as the
//   buffer cache is used up quickly

// default buffer cache
static constexpr int kBufferCount = 4;
static constexpr int kBufferSize = 256 * 1024;

#include "arrow/buffer.h"
#include "arrow/flight/data_plane/internal.h"
#include "arrow/flight/data_plane/serialize.h"
#include "arrow/flight/data_plane/types.h"
#include "arrow/result.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/counting_semaphore.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"

#include <atomic>
#include <cstring>
#include <mutex>
#include <queue>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdatomic.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace arrow {
namespace flight {
namespace internal {

namespace {

// stream map keys transferred together with grpc client metadata
// must be unique, no capital letter
// - name prefix of fifo and shared memory
const char kIpcNamePrefix[] = "flight-dataplane-shm-ipc";

// message between client and server
struct ShmMsg {
  static constexpr uint32_t kMagic = 0xFEEDCAFE;
  static constexpr int kShmNameSize = 64;
  // Get
  // # server -> client: GetData
  // # client -> server: GetAck/Nak
  // # server -> client: GetDone (WritesDone)
  // Put
  // # client -> server: PutData
  // # server -> client: PutAck/Nak
  // # client -> server: PutDone (WritesDone)
  enum Type {
    Min,
    GetData,
    PutData,
    GetAck,
    PutAck,
    GetNak,
    PutNak,
    GetDone,
    PutDone,
    GetInvalid,
    PutInvalid,
    Finish,
    Max
  };

  ShmMsg() = default;

  ShmMsg(Type type, int64_t size, const char* shm_name)
      : magic(kMagic), type(type), size(size) {
    // shm_name strlen is verified in Make
    const int n = snprintf(this->shm_name, kShmNameSize, "%s", shm_name);
    DCHECK(n >= 0 && n < kShmNameSize);
  }

  static arrow::Result<ShmMsg> Make(Type type, int64_t size = 0,
                                    const std::string& shm_name = "") {
    DCHECK(type > Type::Min && type < Type::Max);
    if (shm_name.size() >= kShmNameSize) {
      return Status::IOError("shared memory name length greater than ",
                             int(kShmNameSize));
    }
    return ShmMsg(type, size, shm_name.c_str());
  }

  // passed across process, no pointer
  uint32_t magic = kMagic;
  Type type = Type::Min;
  int64_t size = 0;
  char shm_name[kShmNameSize]{};
};

// shared memory buffer
class ShmBuffer : public MutableBuffer {
 public:
  // create and map a new shared memory with specified name and size, called by client
  static arrow::Result<std::shared_ptr<ShmBuffer>> Create(const std::string& name,
                                                          int64_t size) {
    DCHECK_GT(size, 0);

    int fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0666);
    if (fd == -1) {
      return Status::IOError("create shm: ", strerror(errno));
    }
    if (ftruncate(fd, size) == -1) {
      const int saved_errno = errno;
      close(fd);
      shm_unlink(name.c_str());
      return Status::IOError("ftruncate: ", strerror(saved_errno));
    }

    void* data =
        mmap(NULL, static_cast<size_t>(size), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    const int saved_errno = errno;
    close(fd);
    if (data == MAP_FAILED) {
      shm_unlink(name.c_str());
      return Status::IOError("mmap: ", strerror(saved_errno));
    }

    return std::make_shared<ShmBuffer>(reinterpret_cast<uint8_t*>(data), size);
  }

  // open and map an existing shared memory, called by server
  static arrow::Result<std::shared_ptr<ShmBuffer>> Open(const std::string& name,
                                                        int64_t size) {
    DCHECK_GT(size, 0);

    int fd = shm_open(name.c_str(), O_RDWR, 0666);
    if (fd == -1) {
      return Status::IOError("open shm: ", strerror(errno));
    }

    void* data =
        mmap(NULL, static_cast<size_t>(size), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    const int saved_errno = errno;
    // memory is mapped, we can close shm fd and delete rpc file
    close(fd);
    shm_unlink(name.c_str());
    if (data == MAP_FAILED) {
      return Status::IOError("mmap: ", strerror(saved_errno));
    }

    return std::make_shared<ShmBuffer>(reinterpret_cast<uint8_t*>(data), size);
  }

  ShmBuffer(uint8_t* data, int64_t size) : MutableBuffer(data, size) {}

  ~ShmBuffer() override { munmap(mutable_data(), static_cast<size_t>(size())); }
};

// per stream buffer cache
class ShmCache {
  class CachedBuffer : public MutableBuffer {
   public:
    CachedBuffer(std::shared_ptr<Buffer> parent, atomic_uchar* refcnt)
        : MutableBuffer(parent->mutable_data(), parent->size()),
          parent_(std::move(parent)),
          refcnt_(refcnt) {}

    // decreases reference count on destruction
    ~CachedBuffer() {
      const uint8_t current = atomic_fetch_sub(refcnt_, 1);
      DCHECK(current == 1 || current == 2);
    }

   private:
    // holds the parent buffer as refcnt_ locates there
    std::shared_ptr<Buffer> parent_;
    atomic_uchar* refcnt_;
  };

 public:
  static arrow::Result<std::unique_ptr<ShmCache>> Create(const std::string& name_prefix,
                                                         int buffer_count,
                                                         int buffer_size) {
    return CreateOrOpen(name_prefix, buffer_count, buffer_size, /*create=*/true);
  }

  static arrow::Result<std::unique_ptr<ShmCache>> Open(const std::string& name_prefix,
                                                       int buffer_count,
                                                       int buffer_size) {
    return CreateOrOpen(name_prefix, buffer_count, buffer_size, /*create=*/false);
  }

  ShmCache(const std::string& name_prefix, int64_t buffer_count, int64_t buffer_size,
           int64_t buffer_offset, std::shared_ptr<Buffer> buffer, atomic_uchar* refcnt)
      : name_prefix_(name_prefix),
        buffer_count_(buffer_count),
        buffer_size_(buffer_size),
        buffer_offset_(buffer_offset),
        buffer_(std::move(buffer)),
        refcnt_(refcnt) {}

  // called by writer
  arrow::Result<std::pair<std::string, std::shared_ptr<Buffer>>> CreateBuffer(
      int64_t size, bool client) {
    if (size <= buffer_size_) {
      // acquire free cached buffer
      for (int i = 0; i < buffer_count_; ++i) {
        uint8_t expected = 0;
        if (atomic_compare_exchange_strong(&refcnt_[i], &expected, 1)) {
          auto buffer = std::make_shared<CachedBuffer>(
              SliceMutableBuffer(buffer_, buffer_offset_ + buffer_size_ * i, size),
              &refcnt_[i]);
          // append buffer index to name
          return std::make_pair(cached_buffer_prefix() + std::to_string(i),
                                std::move(buffer));
        }
      }
    }
    // fallback to one-shot buffer if no cached buffer available
    static std::atomic<unsigned> counter{0};
    const std::string name =
        name_prefix_ + (client ? "c" : "s") + std::to_string(++counter);
    ARROW_ASSIGN_OR_RAISE(auto buffer, ShmBuffer::Create(name, size));
    return std::make_pair(name, std::move(buffer));
  }

  // called by reader
  arrow::Result<std::shared_ptr<Buffer>> OpenBuffer(const std::string& shm_name,
                                                    int64_t size) {
    const std::string name_prefix = cached_buffer_prefix();
    if (shm_name.find(name_prefix) == 0) {
      const int i = std::stoi(
          shm_name.substr(name_prefix.size(), shm_name.size() - name_prefix.size()));
      if (i < 0 || i >= buffer_count_) {
        return Status::IOError("invalid buffer index");
      }
      // normally, writer is still holding the buffer, refcnt should be 1
      // but writer stream may have be destroyed which decreased refcnt to 0
      const uint8_t current = atomic_fetch_add(&refcnt_[i], 1);
      DCHECK(current == 0 || current == 1);
      return std::make_shared<CachedBuffer>(
          SliceMutableBuffer(buffer_, buffer_offset_ + buffer_size_ * i, size),
          &refcnt_[i]);
    }
    return ShmBuffer::Open(shm_name, size);
  }

 private:
  static arrow::Result<std::unique_ptr<ShmCache>> CreateOrOpen(
      const std::string& name_prefix, int64_t buffer_count, int64_t buffer_size,
      bool create) {
    if (buffer_count < 0 || buffer_size < 0) {
      return Status::Invalid("invalid buffer count or size");
    }
    if (buffer_count == 0 || buffer_size == 0) {
      return arrow::internal::make_unique<ShmCache>(name_prefix, 0, 0, 0, nullptr,
                                                    nullptr);
    }

    // allocate all buffers at once, prepend refcnt[] array
    buffer_size = bit_util::RoundUpToPowerOf2(buffer_size, 64);
    const int64_t buffer_offset = bit_util::RoundUpToPowerOf2(buffer_count, 64);
    const int64_t total_size = buffer_offset + buffer_size * buffer_count;
    ARROW_ASSIGN_OR_RAISE(auto buffer,
                          create ? ShmBuffer::Create(name_prefix + "cc-shm", total_size)
                                 : ShmBuffer::Open(name_prefix + "cc-shm", total_size));
    atomic_uchar* refcnt = reinterpret_cast<atomic_uchar*>(buffer->mutable_data());
    if (create) {
      std::memset(refcnt, 0, buffer_count);
    }
    return arrow::internal::make_unique<ShmCache>(
        name_prefix, buffer_count, buffer_size, buffer_offset, std::move(buffer), refcnt);
  }

  std::string cached_buffer_prefix() { return name_prefix_ + "cc-buf"; }

  const std::string name_prefix_;
  // shared buffer
  // - all cached buffers are allocate in one continuous buffer
  // - prepends refcnt[] array with each element refers to one buffer
  //   * shared and accessed as atomic variables by client/server
  //   * acquiring steps: 0 - free, 1 - created by writer, 2 - opened by reader
  //   * releasing steps: both writer and reader decreased refcnt by 1
  // - memory layout
  //           +----------------+--------------+-----+------------------+
  //   content | refcnt[count_] | buffer0      | ... | buffer[count_-1] |
  //           +----------------+--------------+-----+------------------+
  //      size | buffer_count_  | buffer_size_ | ... | buffer_size_     |
  //           +----------------+--------------+-----+------------------+
  //           ^                ^
  //           |                |
  //           0                buffer_offset_
  const int64_t buffer_count_, buffer_size_, buffer_offset_;
  std::shared_ptr<Buffer> buffer_;
  atomic_uchar* refcnt_;
  static_assert(sizeof(atomic_uchar) == 1, "");
};

// fifo for client and server messages
class ShmFifo {
 public:
  // create and open two fifos for read/write, called by client
  static arrow::Result<std::unique_ptr<ShmFifo>> Create(
      const std::string& fifo_name, std::unique_ptr<ShmCache>&& shm_cache) {
    const std::string reader_path = "/tmp/" + fifo_name + "s2c";
    const std::string writer_path = "/tmp/" + fifo_name + "c2s";
    if (mkfifo(reader_path.c_str(), 0666) == -1) {
      return Status::IOError("mkfifo: ", strerror(errno));
    }
    if (mkfifo(writer_path.c_str(), 0666) == -1) {
      unlink(reader_path.c_str());
      return Status::IOError("mkfifo: ", strerror(errno));
    }
    // fifo open hangs if RDONLY or WRONLY (until peer opens)
    int reader_fd = open(reader_path.c_str(), O_RDWR);
    int writer_fd = open(writer_path.c_str(), O_RDWR);
    if (reader_fd == -1 || writer_fd == -1) {
      unlink(reader_path.c_str());
      unlink(writer_path.c_str());
      if (reader_fd != -1) close(reader_fd);
      if (writer_fd != -1) close(writer_fd);
      return Status::IOError("create fifo");
    }
    return arrow::internal::make_unique<ShmFifo>(std::move(shm_cache), reader_fd,
                                                 writer_fd);
  }

  // open existing fifos, called by server
  static arrow::Result<std::unique_ptr<ShmFifo>> Open(
      const std::string& fifo_name, std::unique_ptr<ShmCache>&& shm_cache) {
    const std::string reader_path = "/tmp/" + fifo_name + "c2s";
    const std::string writer_path = "/tmp/" + fifo_name + "s2c";
    int reader_fd = open(reader_path.c_str(), O_RDWR);
    int writer_fd = open(writer_path.c_str(), O_RDWR);
    // we've opened the fifos, unlink fifo files
    unlink(reader_path.c_str());
    unlink(writer_path.c_str());
    if (reader_fd == -1 || writer_fd == -1) {
      if (reader_fd != -1) close(reader_fd);
      if (writer_fd != -1) close(writer_fd);
      return Status::IOError("open fifos");
    }
    return arrow::internal::make_unique<ShmFifo>(std::move(shm_cache), reader_fd,
                                                 writer_fd);
  }

  ShmFifo(std::unique_ptr<ShmCache>&& shm_cache, int reader_fd, int writer_fd)
      : shm_cache_(std::move(shm_cache)), reader_fd_(reader_fd), writer_fd_(writer_fd) {
    pipe(pipe_fds_);
    reader_thread_ = std::thread([this] { ReaderThread(); });
  }

  ~ShmFifo() {
    StopReaderThread();
    close(pipe_fds_[0]);
    close(pipe_fds_[1]);
    close(reader_fd_);
    // XXX: if writer fifo closes immediately after writing finish message
    // the reader fifo may not see the messag and timeout (both ends must
    // be open for fifo to work properly)
    // it won't happen after replacing fifo with unix socket
    // below horrible code is a temporary workaround for this issue
    int writer_fd = writer_fd_;
    std::thread([writer_fd]() {
      sleep(1);
      close(writer_fd);
    }).detach();
  }

  Status WriteData(ShmMsg::Type msg_type, std::shared_ptr<Buffer> buffer,
                   const std::string& shm_name) {
    DCHECK(msg_type == ShmMsg::GetData || msg_type == ShmMsg::PutData);
    if (peer_finished_) {
      return Status::IOError("peer finished or cancelled");
    }

    const int64_t size = buffer->size();
    ARROW_ASSIGN_OR_RAISE(ShmMsg msg, ShmMsg::Make(msg_type, size, shm_name));

    // hold write buffer in held_writers_ map to wait for peer response
    // it must be done before writing os fifo, in case peer responses fast
    {
      const std::lock_guard<std::mutex> lock(held_writers_mtx_);
      DCHECK_EQ(held_writers_.find(shm_name), held_writers_.end());
      held_writers_[shm_name] = std::move(buffer);
    }

    // write to os fifo
    const Status st = OsWriteFifo(msg);
    if (!st.ok()) {
      // release the buffer on error
      const std::lock_guard<std::mutex> lock(held_writers_mtx_);
      const auto n = held_writers_.erase(shm_name);
      DCHECK_EQ(n, 1);
    }
    return st;
  }

  Status WriteCtrl(ShmMsg::Type msg_type) {
    ARROW_ASSIGN_OR_RAISE(ShmMsg msg, ShmMsg::Make(msg_type));
    return OsWriteFifo(msg);
  }

  arrow::Result<std::pair<ShmMsg::Type, std::shared_ptr<Buffer>>> ReadMsg() {
    RETURN_NOT_OK(reader_sem_.Acquire(1));
    const std::lock_guard<std::mutex> lock(reader_queue_mtx_);
    auto v = reader_queue_.front();
    reader_queue_.pop();
    return std::move(v);
  }

  arrow::Result<std::pair<std::string, std::shared_ptr<Buffer>>> CreateBuffer(
      int64_t size, bool client) {
    return shm_cache_->CreateBuffer(size, client);
  }

 private:
  void ReaderThread() {
    ShmMsg msg;
    while (OsReadFifo(&msg).ok()) {
      DCHECK_EQ(msg.magic, ShmMsg::kMagic);

      switch (msg.type) {
        // data
        case ShmMsg::GetData:
        case ShmMsg::PutData:
          // create buffer per received message, append to reader queue
          {
            ShmMsg::Type msg_type = msg.type;
            std::shared_ptr<Buffer> buffer;
            auto result = shm_cache_->OpenBuffer(msg.shm_name, msg.size);
            if (result.ok()) {
              buffer = result.ValueOrDie();
            } else {
              msg_type =
                  msg.type == ShmMsg::GetData ? ShmMsg::GetInvalid : ShmMsg::PutInvalid;
            }
            {
              std::lock_guard<std::mutex> lock(reader_queue_mtx_);
              reader_queue_.emplace(msg_type, std::move(buffer));
            }
            ARROW_UNUSED(reader_sem_.Release(1));
            // send response so the writer can free its buffer
            if (msg.type == ShmMsg::GetData) {
              msg.type = result.ok() ? ShmMsg::GetAck : ShmMsg::GetNak;
            } else {
              msg.type = result.ok() ? ShmMsg::PutAck : ShmMsg::PutNak;
            }
            DCHECK_OK(OsWriteFifo(msg));
          }
          break;
        // writes done, error, finish
        case ShmMsg::GetDone:
        case ShmMsg::PutDone:
        case ShmMsg::GetInvalid:
        case ShmMsg::PutInvalid:
        case ShmMsg::Finish: {
          const std::lock_guard<std::mutex> lock(reader_queue_mtx_);
          reader_queue_.emplace(msg.type, std::shared_ptr<Buffer>());
        }
          ARROW_UNUSED(reader_sem_.Release(1));
          if (msg.type == ShmMsg::Finish) {
            peer_finished_ = true;
          }
          break;
        // data response
        case ShmMsg::GetAck:
        case ShmMsg::GetNak:
        case ShmMsg::PutAck:
        case ShmMsg::PutNak:
          // release according write buffer
          {
            const std::lock_guard<std::mutex> lock(held_writers_mtx_);
            const auto n = held_writers_.erase(msg.shm_name);
            DCHECK_EQ(n, 1);
          }
          break;
        default:
          DCHECK(false);
          break;
      }

      std::memset(&msg, 0, sizeof(ShmMsg));
    }
  }

  // make sure to write/read a full message per call
  Status OsWriteFifo(const ShmMsg& msg) {
    if (fifo_write_error_) {
      return Status::IOError("fifo write error");
    }
    const uint8_t* buf = reinterpret_cast<const uint8_t*>(&msg);
    size_t count = sizeof(ShmMsg);
    while (count > 0) {
      ssize_t ret = write(writer_fd_, buf, count);
      if (ret == -1 && errno != EINTR) {
        fifo_write_error_ = true;
        return Status::IOError("write: ", strerror(errno));
      }
      count -= ret;
      buf += ret;
    }
    return Status::OK();
  }

  Status OsReadFifo(ShmMsg* msg) {
    struct pollfd fds[2];
    fds[0].fd = reader_fd_;
    fds[0].events = POLLIN;
    fds[1].fd = pipe_fds_[0];
    fds[1].events = POLLIN;

    uint8_t* buf = reinterpret_cast<uint8_t*>(msg);
    size_t count = sizeof(ShmMsg);
    while (count > 0) {
      // force checking stop token every 5 seconds, in case pipe method fails
      if (poll(fds, 2, 5000) == -1) {
        if (errno == EINTR) {
          continue;
        }
        return Status::IOError("poll: ", strerror(errno));
      }
      if ((fds[1].revents & POLLIN) || stop_) {
        // exit thread if pipe received something or stop token is set
        return Status::IOError("stop requested");
      }
      if (fds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) {
        return Status::IOError("error polled");
      }
      if (fds[0].revents & POLLIN) {
        ssize_t ret = read(reader_fd_, buf, count);
        count -= ret;
        buf += ret;
      }
    }
    return Status::OK();
  }

  void StopReaderThread() {
    stop_ = true;
    while (write(pipe_fds_[1], "s", 1) == -1 && errno == EINTR) {
    }
    reader_thread_.join();
  }

  std::unique_ptr<ShmCache> shm_cache_;
  // os fifo fd
  int reader_fd_, writer_fd_;
  bool fifo_write_error_{false};
  std::thread reader_thread_;
  // self pipe to stop reader thread
  int pipe_fds_[2];
  // force stoping reader thread in case pipe method fails
  std::atomic<bool> stop_{false};
  // read message queue with timeout (XXX: suitable value?)
  arrow::util::CountingSemaphore reader_sem_{/*initial=*/0, /*timeout_seconds=*/5};
  std::queue<std::pair<ShmMsg::Type, std::shared_ptr<Buffer>>> reader_queue_;
  std::mutex reader_queue_mtx_;
  // write buffers cannot be freed before peer response
  std::unordered_map<std::string, std::shared_ptr<Buffer>> held_writers_;
  std::mutex held_writers_mtx_;
  // received finish or cancel message, cannot write anymore
  std::atomic<bool> peer_finished_{false};
};

struct ShmStreamImpl {
  ShmStreamImpl(bool client, StreamType stream_type, std::unique_ptr<ShmFifo>&& shm_fifo)
      : client_(client), stream_type_(stream_type), shm_fifo_(std::move(shm_fifo)) {}

  Status Read(FlightData* data) {
    DCHECK_NE(stream_type_, client_ ? StreamType::kPut : StreamType::kGet);
    const std::string prefix = client_ ? "client: " : "server: ";
    if (reads_done_) {
      return Status::IOError(prefix, "reads done");
    }
    ShmMsg::Type msg_type;
    std::shared_ptr<Buffer> buffer;
    ARROW_ASSIGN_OR_RAISE(std::tie(msg_type, buffer), shm_fifo_->ReadMsg());
    switch (msg_type) {
      case ShmMsg::GetData:
      case ShmMsg::PutData:
        DCHECK_EQ(msg_type == ShmMsg::GetData, client_);
        return Deserialize(std::move(buffer), data);
      case ShmMsg::GetDone:
      case ShmMsg::PutDone:
        DCHECK_EQ(msg_type == ShmMsg::GetDone, client_);
        reads_done_ = true;
        return Status::IOError(prefix, "peer done writing");
      case ShmMsg::GetInvalid:
      case ShmMsg::PutInvalid:
        DCHECK_EQ(msg_type == ShmMsg::GetInvalid, client_);
        return Status::Invalid(prefix, "recevied invalid payload");
      case ShmMsg::Finish:
        DCHECK(!client_);
        reads_done_ = writes_done_ = true;
        return Status::IOError(prefix, "client finished");
      default:
        DCHECK(false);
        return Status::Invalid(prefix, "received invalid message");
    }
  }

  Status Write(const FlightPayload& payload) {
    DCHECK_NE(stream_type_, client_ ? StreamType::kGet : StreamType::kPut);
    if (writes_done_) {
      return Status::IOError(client_ ? "client" : "server", ": writes done");
    }
    int64_t total_size;
    auto result = Serialize(payload, &total_size);
    if (!result.ok()) {
      ARROW_UNUSED(
          shm_fifo_->WriteCtrl(client_ ? ShmMsg::PutInvalid : ShmMsg::GetInvalid));
      return result.status();
    }
    DCHECK_GT(total_size, 0);
    const std::vector<SerializeSlice>& slices = result.ValueOrDie();

    std::string shm_name;
    std::shared_ptr<Buffer> buffer;
    ARROW_ASSIGN_OR_RAISE(std::tie(shm_name, buffer),
                          shm_fifo_->CreateBuffer(total_size, client_));
    CopySlicesToBuffer(slices, buffer.get());
    return shm_fifo_->WriteData(client_ ? ShmMsg::PutData : ShmMsg::GetData,
                                std::move(buffer), shm_name);
  }

  Status WritesDone() {
    DCHECK_NE(stream_type_, client_ ? StreamType::kGet : StreamType::kPut);
    if (writes_done_) {
      return Status::OK();
    }
    writes_done_ = true;
    return shm_fifo_->WriteCtrl(client_ ? ShmMsg::PutDone : ShmMsg::GetDone);
  }

  static void CopySlicesToBuffer(const std::vector<SerializeSlice>& slices,
                                 Buffer* buffer) {
    uint8_t* dest_ptr = buffer->mutable_data();
    for (const auto& slice : slices) {
      DCHECK_LE(dest_ptr + slice.size(), buffer->data() + buffer->size());
      std::memcpy(dest_ptr, slice.data(), static_cast<size_t>(slice.size()));
      dest_ptr += slice.size();
    }
    DCHECK_EQ(dest_ptr, buffer->data() + buffer->size());
  }

  const bool client_;
  const StreamType stream_type_;
  std::unique_ptr<ShmFifo> shm_fifo_;
  std::atomic<bool> reads_done_{false}, writes_done_{false};
};

class ShmClientStream : public DataClientStream {
 public:
  ShmClientStream(StreamType stream_type, std::unique_ptr<ShmFifo>&& shm_fifo)
      : stream_(/*client=*/true, stream_type, std::move(shm_fifo)) {}

  ~ShmClientStream() { ARROW_UNUSED(Finish()); }

  Status Read(FlightData* data) override { return stream_.Read(data); }
  Status Write(const FlightPayload& payload) override { return stream_.Write(payload); }
  Status WritesDone() override { return stream_.WritesDone(); }

  Status Finish() override {
    stream_.writes_done_ = stream_.reads_done_ = true;
    return stream_.shm_fifo_->WriteCtrl(ShmMsg::Finish);
  }

  void TryCancel() override {
    ARROW_UNUSED(stream_.shm_fifo_->WriteCtrl(ShmMsg::Finish));
  }

 private:
  ShmStreamImpl stream_;
};

class ShmServerStream : public DataServerStream {
 public:
  ShmServerStream(StreamType stream_type, std::unique_ptr<ShmFifo>&& shm_fifo)
      : stream_(/*client=*/false, stream_type, std::move(shm_fifo)) {}

  ~ShmServerStream() {
    if (stream_.stream_type_ != StreamType::kPut) {
      ARROW_UNUSED(WritesDone());
    }
  }

  Status Read(FlightData* data) override { return stream_.Read(data); }
  Status Write(const FlightPayload& payload) override { return stream_.Write(payload); }
  Status WritesDone() override { return stream_.WritesDone(); }

 private:
  ShmStreamImpl stream_;
};

class ShmClientDataPlane : public ClientDataPlane {
 private:
  ResultClientStream DoGetImpl(StreamMap* map) override {
    return Do(map, StreamType::kGet);
  }

  ResultClientStream DoPutImpl(StreamMap* map) override {
    return Do(map, StreamType::kPut);
  }

  ResultClientStream DoExchangeImpl(StreamMap* map) override {
    return Do(map, StreamType::kExchange);
  }

  ResultClientStream Do(StreamMap* map, StreamType stream_type) {
    const std::string name_prefix = GenerateNamePrefix();
    (*map)[kIpcNamePrefix] = name_prefix;
    ARROW_ASSIGN_OR_RAISE(auto shm_cache,
                          ShmCache::Create(name_prefix, kBufferCount, kBufferSize));
    ARROW_ASSIGN_OR_RAISE(auto shm_fifo,
                          ShmFifo::Create(name_prefix, std::move(shm_cache)));
    return arrow::internal::make_unique<ShmClientStream>(stream_type,
                                                         std::move(shm_fifo));
  }

  // generate system unique name prefix for ipc objects
  // prefix = "/flight-shm-{client pid}-{stream counter}-"
  std::string GenerateNamePrefix() {
    static std::atomic<unsigned> counter{0};
    std::stringstream name_prefix;
    name_prefix << "/flight-shm-" << getpid() << '-' << ++counter << '-';
    return name_prefix.str();
  }
};

class ShmServerDataPlane : public ServerDataPlane {
 private:
  ResultServerStream DoGetImpl(const StreamMap& map) override {
    return Do(map, StreamType::kGet);
  }

  ResultServerStream DoPutImpl(const StreamMap& map) override {
    return Do(map, StreamType::kPut);
  }

  ResultServerStream DoExchangeImpl(const StreamMap& map) override {
    return Do(map, StreamType::kExchange);
  }

  std::vector<std::string> stream_keys() override { return {kIpcNamePrefix}; }

  ResultServerStream Do(const StreamMap& map, StreamType stream_type) {
    ARROW_ASSIGN_OR_RAISE(auto name_prefix, GetNamePrefix(map));
    ARROW_ASSIGN_OR_RAISE(auto shm_cache,
                          ShmCache::Open(name_prefix, kBufferCount, kBufferSize));
    ARROW_ASSIGN_OR_RAISE(auto shm_fifo,
                          ShmFifo::Open(name_prefix, std::move(shm_cache)));
    return arrow::internal::make_unique<ShmServerStream>(stream_type,
                                                         std::move(shm_fifo));
  }

  // extract ipc objecs name prefix from stream map set by client
  arrow::Result<std::string> GetNamePrefix(const StreamMap& map) {
    auto it = map.find(kIpcNamePrefix);
    if (it == map.end()) {
      return Status::Invalid("key not found: ", kIpcNamePrefix);
    }
    return it->second;
  }
};

arrow::Result<std::unique_ptr<ClientDataPlane>> MakeShmClientDataPlane(
    const std::string&) {
  return arrow::internal::make_unique<ShmClientDataPlane>();
}

arrow::Result<std::unique_ptr<ServerDataPlane>> MakeShmServerDataPlane(
    const std::string&) {
  return arrow::internal::make_unique<ShmServerDataPlane>();
}

}  // namespace

DataPlaneMaker GetShmDataPlaneMaker() {
  return {MakeShmClientDataPlane, MakeShmServerDataPlane};
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow

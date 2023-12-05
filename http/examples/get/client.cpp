#include <curl/curl.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <chrono>

struct MemoryStruct {
  char *memory;
  size_t size;
};
 
static size_t
WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
  size_t realsize = size * nmemb;
  struct MemoryStruct *mem = (struct MemoryStruct *)userp;
 
  char *ptr = static_cast<char*>(realloc(mem->memory, mem->size + realsize + 1));
  if(!ptr) {
    printf("out of memory\n");
    return 0;
  }
 
  mem->memory = ptr;
  memcpy(&(mem->memory[mem->size]), contents, realsize);
  mem->size += realsize;
  mem->memory[mem->size] = 0;
 
  return realsize;
}

int main(void)
{
  std::string url = "http://localhost:8000";

  CURL *curl_handle;
  CURLcode res;
 
  struct MemoryStruct chunk;
 
  chunk.memory = static_cast<char*>(malloc(1));
  chunk.size = 0;

  curl_global_init(CURL_GLOBAL_ALL);
  curl_handle = curl_easy_init();

  curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);
 
  auto start_time = std::chrono::steady_clock::now();

  res = curl_easy_perform(curl_handle);

  printf("%lu bytes received\n", (unsigned long)chunk.size);

  auto buffer = arrow::Buffer::Wrap(reinterpret_cast<const uint8_t *>(chunk.memory), chunk.size);
  auto input_stream = std::make_shared<arrow::io::BufferReader>(buffer);
  auto record_batch_reader = arrow::ipc::RecordBatchStreamReader::Open(input_stream).ValueOrDie();

  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  std::shared_ptr<arrow::RecordBatch> record_batch;
  while (record_batch_reader->ReadNext(&record_batch).ok() && record_batch)
  {
      record_batches.push_back(record_batch);
  }

  printf("%lu record batches received\n", (unsigned long)(record_batches.size()));

  auto end_time = std::chrono::steady_clock::now();

  auto time_duration = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  printf("%.2f seconds elapsed\n", time_duration.count());

  curl_easy_cleanup(curl_handle);
 
  free(chunk.memory);

  curl_global_cleanup();
 
  return 0;
}

#include <curl/curl.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <chrono>

static size_t
WriteFunction(void *contents, size_t size, size_t nmemb, void *userp)
{
  size_t real_size = size * nmemb;
  auto decoder = static_cast<arrow::ipc::StreamDecoder*>(userp);
  if (decoder->Consume(static_cast<const uint8_t*>(contents), real_size).ok()) {
    return real_size;
  } else {
    return 0;
  }
}

int main(void)
{
  std::string url = "http://localhost:8000";

  CURL *curl_handle;
  CURLcode res;

  // We use arrow::ipc::CollectListner() here for simplicity,
  // but another option is to process decoded record batches
  // as a stream by overriding arrow::ipc::Listener().
  auto collect_listener = std::make_shared<arrow::ipc::CollectListener>();
  arrow::ipc::StreamDecoder decoder(collect_listener);

  curl_global_init(CURL_GLOBAL_ALL);
  curl_handle = curl_easy_init();

  curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteFunction);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &decoder);
 
  auto start_time = std::chrono::steady_clock::now();

  res = curl_easy_perform(curl_handle);

  printf("%lld record batches received\n", collect_listener->num_record_batches());

  auto end_time = std::chrono::steady_clock::now();

  auto time_duration = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  printf("%.2f seconds elapsed\n", time_duration.count());

  curl_easy_cleanup(curl_handle);
  curl_global_cleanup();

  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  record_batches = collect_listener->record_batches();
 
  return 0;
}

// to compile (for example):
//clang++ client.cpp -std=c++17 $(pkg-config --cflags --libs arrow libcurl) -o client

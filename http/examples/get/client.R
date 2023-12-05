library(httr)
library(arrow)
library(tictoc)

url <- 'http://localhost:8000'

tic()

response <- GET(url)
buffer <- content(response, "raw")
reader <- RecordBatchStreamReader$create(buffer)
table <- reader$read_table()

# or:
#batches <- reader$batches()
# but this is very slow

# or:
#result <- read_ipc_stream(buffer, as_data_frame = FALSE)

# or:
#result <- read_ipc_stream('http://localhost:8000', as_data_frame = FALSE)

toc()

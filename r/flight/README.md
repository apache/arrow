
# flight

<!-- badges: start -->
<!-- badges: end -->

[**Flight**](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/)
is a general-purpose client-server framework for high performance
transport of large datasets over network interfaces, built as part of the
[Apache Arrow](https://arrow.apache.org) project.
The `flight` package provides methods for connecting to Flight RPC servers
to send and receive data.

## Installation

This is highly experimental, but if you want to try it,

``` r
remotes::install_github("apache/arrow/r/flight")
```

Before using the first time, you'll need to install `pyarrow` as well:

```r
arrow::install_pyarrow()
```

## Example

``` r
# In one process, start the demo server
library(flight)
demo_server <- load_flight_server("demo_server")
server <- demo_server$DemoFlightServer(port = 8089)
server$serve()

# In a different process, connect to it
library(flight)
client <- flight_connect(port = 8089)
# Upload some data to our server so there's something to demo
push_data(client, iris, path = "test_data/iris")
# ^D

# In a fresh process, let's connect to our server and pull data
library(flight)
library(arrow)
library(dplyr)
client <- flight_connect(port = 8089)
client %>%
  flight_get("test_data/iris") %>%
  group_by(Species) %>%
  summarize(max_petal = max(Petal.Length))

## # A tibble: 3 x 2
##   Species    max_petal
##   <fct>          <dbl>
## 1 setosa           1.9
## 2 versicolor       5.1
## 3 virginica        6.9
```

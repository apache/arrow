package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/apache/arrow/go/arrow/internal/flight_integration"
	"google.golang.org/grpc"
)

var (
	host     = flag.String("host", "localhost", "Server host to connect to")
	port     = flag.Int("port", 31337, "Server port to connect to")
	path     = flag.String("path", "", "Resource path to request")
	scenario = flag.String("scenario", "", "Integration test scenario to run")
)

const retries = 3

func main() {
	flag.Parse()

	c := flight_integration.GetScenario(*scenario)
	var err error
	for i := 0; i < retries; i++ {
		err = c.RunClient(fmt.Sprintf("%s:%d", *host, *port), grpc.WithInsecure())
		if err == nil {
			break
		}
		time.Sleep(time.Duration(i+1) * 500 * time.Millisecond)
	}
	if err != nil {
		panic(err)
	}
}

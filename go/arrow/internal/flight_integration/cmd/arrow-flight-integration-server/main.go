package main

import (
	"flag"
	"fmt"
	"os"
	"syscall"

	"github.com/apache/arrow/go/arrow/internal/flight_integration"
)

var (
	port     = flag.Int("port", 31337, "Server port to listen on")
	scenario = flag.String("scenario", "", "Integration test scenario to run")
)

func main() {
	flag.Parse()

	s := flight_integration.GetScenario(*scenario)
	srv := s.MakeServer(*port)
	srv.Init(fmt.Sprintf("0.0.0.0:%d", *port))
	srv.SetShutdownOnSignals(syscall.SIGTERM, os.Interrupt)
	fmt.Println("Server listening on localhost:", *port)
	srv.Serve()
}

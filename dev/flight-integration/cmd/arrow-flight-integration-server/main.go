package main

import (
	"flag"
	"fmt"
	"integration"
	"log"
	"net"
	"strings"

	_ "integration/cases"
	"integration/scenario"
)

var (
	port         = flag.Int("port", 31337, "Server port to listen on")
	scenariosStr = flag.String("scenarios", "", "Comma-delimited scenarios to run")
)

func main() {
	flag.Parse()
	scenarioNames := strings.Split(*scenariosStr, ",")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen on port %d: %v", *port, err)
	}

	scenarios, err := scenario.GetScenarios(scenarioNames...)
	if err != nil {
		log.Fatal(err)
	}

	srv := integration.NewIntegrationServer(scenarios...)

	log.Printf("server listening at %v", lis.Addr())
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

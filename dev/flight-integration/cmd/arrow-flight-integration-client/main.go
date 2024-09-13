package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	_ "integration/cases"
	"integration/scenario"
)

var (
	host         = flag.String("host", "localhost", "Server host to connect to")
	port         = flag.Int("port", 31337, "Server port to connect to")
	scenariosStr = flag.String("scenarios", "", "Comma-delimited scenarios to run")
)

func main() {
	flag.Parse()
	addr := fmt.Sprintf("%s:%d", *host, *port)
	scenarioNames := strings.Split(*scenariosStr, ",")

	scenarios, err := scenario.GetScenarios(scenarioNames...)
	if err != nil {
		log.Fatal(err)
	}

	dialServer := func() (conn *grpc.ClientConn, err error) {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	runner := scenario.NewRunner(scenarios)
	if err := runner.RunScenarios(dialServer); err != nil {
		log.Fatal(err)
	}
}

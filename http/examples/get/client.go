package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

func main() {
	start := time.Now()
	resp, err := http.Get("http://localhost:8000")
	if err != nil {
		panic(err)
	}

	if resp.StatusCode != http.StatusOK {
		panic(fmt.Errorf("got non-200 status: %d", resp.StatusCode))
	}
	defer resp.Body.Close()

	rdr, err := ipc.NewReader(resp.Body, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		panic(err)
	}
	defer rdr.Release()

	batches := make([]arrow.Record, 0)
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		batches = append(batches, rec)
	}

	if rdr.Err() != nil {
		panic(rdr.Err())
	}

	execTime := time.Since(start)

	fmt.Printf("%d record batches received\n", len(batches))
	fmt.Printf("%.2f seconds elapsed\n", execTime.Seconds())
}

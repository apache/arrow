package main

import (
	"fmt"
	"log"

	"github.com/apache/arrow/go/dataset"
)

func main() {
	dsf, err := dataset.CreateDatasetFactory("file:///workspace/example.parquet")
	if err != nil {
		log.Fatal(err)
	}

	// fmt.Println(dsf.Inspect(1))
	ds, err := dsf.CreateDataset()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(ds.Schema())
	fmt.Println(ds.Type())
}

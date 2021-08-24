package main

import (
	"fmt"
	"io"
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

	scanner, err := ds.NewScan([]string{"symbol", "fund_category_asset_class"}, 8192)
	if err != nil {
		log.Fatal(err)
	}

	rdr, err := scanner.GetReader()
	if err != nil {
		log.Fatal(err)
	}

	for {
		rec, err := rdr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		fmt.Println(rec)
		rec.Release()
	}
}

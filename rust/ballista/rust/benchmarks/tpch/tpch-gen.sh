#!/bin/bash

BALLISTA_VERSION=0.4.2-SNAPSHOT

#set -e

docker build -t ballistacompute/ballista-tpchgen:$BALLISTA_VERSION -f tpchgen.dockerfile .

# Generate data into the ./data directory if it does not already exist
FILE=./data/supplier.tbl
if test -f "$FILE"; then
    echo "$FILE exists."
else
  mkdir data 2>/dev/null
  docker run -v `pwd`/data:/data -it --rm ballistacompute/ballista-tpchgen:$BALLISTA_VERSION
  ls -l data
fi
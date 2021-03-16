#!/bin/bash
set -e

# This bash script is meant to be run inside the docker-compose environment. Check the README for instructions

for query in 1 3 5 6 10 12
do
  /tpch benchmark --host ballista-scheduler --port 50050 --query $query --path /data --format tbl --iterations 1 --debug
done

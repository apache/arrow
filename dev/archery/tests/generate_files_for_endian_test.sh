#!/bin/bash
arrow_dir=<top directory of cloned Arrow repository>

json_file=`archery integration --with-cpp=1 | grep "Testing file" | tail -1 | awk '{ print $3 }'`
json_dir=`dirname $json_file`
for f in $json_dir/*.json ; do $arrow_dir/cpp/build/debug/arrow-json-integration-test -mode JSON_TO_ARROW -json $f -arrow ${f%.*}.arrow_file -integration true ; done
for f in $json_dir/*.arrow_file ; do $arrow_dir/cpp/build/debug/arrow-file-to-stream $f > ${f%.*}.stream; done
for f in $json_dir/*.json ; do gzip $f ; done
echo "The files are under $json_dir"

#!/bin/bash

echo "Compiling flatbuffer schemas..."
#flatc -o lib --js ../format/Message.fbs ../format/File.fbs
flatc -o lib --js ../format/*.fbs
cat lib/*_generated.js > lib/Arrow_generated.js

#!/bin/bash

INDIR=`dirname $2`
INNAME=`basename $2`
OUTDIR=`dirname $4`
OUTNAME=`basename $4`

docker run --rm -u `id -u`:`id -g` -v "$INDIR:/data" -v "$OUTDIR:/dataout" minlag/mermaid-cli $1 "/data/$INNAME" $3 "/dataout/$OUTNAME"

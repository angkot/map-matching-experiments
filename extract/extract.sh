#!/bin/bash

INPUT=$1
OUTPUT=$2
BBOX="$(cat $3)"

set -x

./osmconvert $INPUT \
    -o=$OUTPUT \
    -b=$BBOX \
    --complete-ways \
    --drop-author


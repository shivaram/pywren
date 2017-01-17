#!/bin/bash

if [[ $# -ne 1 ]];
then
	echo "Usage serve.sh <dir>"
	exit -1
fi
 
ssd_dir=$1

pushd $ssd_dir

for port in `seq 8080 8111`
do
	python -m SimpleHTTPServer $port &
done

wait

popd

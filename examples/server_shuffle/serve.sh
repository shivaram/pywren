#!/bin/bash

if [[ $# -ne 2 ]];
then
	echo "Usage serve.sh <dir1> <max_port>"
	exit -1
fi
 
ssd_dir=$1

pushd $ssd_dir >/dev/null

set +bm

for port in `seq 8080 $2`
do
	python -m SimpleHTTPServer $port > /dev/null 2>&1 &
done

# wait

# popd

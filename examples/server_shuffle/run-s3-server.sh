#!/bin/bash

if [[ $# -ne 1 ]];
then
	echo "Usage run.sh <max_port>"
	exit -1
fi

pushd /home/ec2-user/pywren/examples > /dev/null

source ~/.bash_profile

for port in `seq 8080 $1`
do
	python s3_server_shuffle_benchmark.py shuffle --mb_per_file 20 --workers ~/workers --port $port --bucket_name shivaram-pywren-test 2>&1 | tee ~/s3_$port.log &
done

wait

popd > /dev/null

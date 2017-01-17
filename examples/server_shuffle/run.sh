#!/bin/bash

if [[ $# -ne 1 ]];
then
	echo "Usage run.sh <max_port>"
	exit -1
fi

pushd /home/ec2-user/pywren/examples > /dev/null

for port in `seq 8080 $1`
do
	python ssd_shuffle_benchmark.py shuffle --mb_per_file 20 --workers ~/workers --port $port --shuffle_dir_name /media/ephemeral0 2>&1 | tee ~/ssd_$port.log &
done

wait

popd > /dev/null

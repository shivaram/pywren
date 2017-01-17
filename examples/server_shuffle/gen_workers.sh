#!/bin/bash

if [[ $# -ne 1 ]];
then
	echo "Usage gen_workers.sh <max_port>"
	exit -1
fi

rm ~/workers

for mc in `cat ~/slaves`
do 
	for port in `seq 8080 $1`
	do
		echo "$mc:$port" >> ~/workers
	done
done

~/copy-dir.sh ~/workers

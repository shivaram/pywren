#!/bin/bash

for port in `seq 8080 8111`
do
	python ssd_shuffle_benchmark.py shuffle --mb_per_file 10 --workers ~/workers --port $port --shuffle_dir_name /media/ephemeral0 &
done

wait

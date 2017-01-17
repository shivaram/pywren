#!/bin/bash

MAX_PORT=8095

bash gen_workers.sh $MAX_PORT
~/copy-dir.sh run-s3-server.sh

sleep 5

bash ~/slaves.sh bash ~/pywren/examples/server_shuffle/run-s3-server.sh $MAX_PORT

#bash ~/slaves.sh killall python

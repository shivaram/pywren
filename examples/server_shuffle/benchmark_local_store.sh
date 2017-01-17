#!/bin/bash

MAX_PORT=8095

bash gen_workers.sh $MAX_PORT
~/copy-dir.sh run.sh
~/copy-dir.sh serve.sh

bash ~/slaves.sh rm -rf /media/ephemeral0/shuffle_*
bash ~/slaves.sh rm -rf /mnt2/shuffle_*

bash ~/slaves.sh bash pywren/examples/server_shuffle/serve.sh /media/ephemeral0 $MAX_PORT &

sleep 5

bash ~/slaves.sh bash ~/pywren/examples/server_shuffle/run.sh $MAX_PORT

bash ~/slaves.sh killall python 2>&1 > /dev/null

"""
Benchmark shuffles using local SSD S3

Run this on every server. 
$ python ssd_shuffle_benchmark.py shuffle --workers=<workers-file-name> --mb_per_file=1000

Saves the output in a pickled list of IO transfer times. 

"""

import time
import boto3 
import botocore
import uuid
import httplib

import time
import pywren
import subprocess
import logging
import sys
import hashlib
import cPickle as pickle
import click
import exampleutils
import socket

from collections import deque

@click.group()
def cli():
    pass

@cli.command()
@click.option('--mb_per_file', help='MB of each object in S3', type=float)
@click.option('--workers', help='file name containing workers', type=str)
@click.option('--port', help='port number for http server to listen', type=int)
@click.option('--shuffle_dir_name', help='dir to save shuffle files in', default='/mnt')
@click.option('--outfile', default='ssd_benchmark.shuffle.output.pickle',
              help='filename to save results in')
def shuffle(mb_per_file, workers, port, shuffle_dir_name, outfile):

    print "mb_per_file =", mb_per_file
    print "workers=", workers
    print "shuffle_dir_name =", shuffle_dir_name
    my_hostname = socket.gethostname().strip()
    print "my hostname ", my_hostname

    bytes_n = int(mb_per_file * 1024**2)
    block_size = 1024 * 1024

    hostname_id_map = {}
    id_cur = 0
    my_worker_id = -1
    with open(workers, 'r') as f:
      lines = f.readlines()
      for line in lines:
          host_port = line.strip().split(':')
          hostname_id_map[id_cur] = (host_port[0], host_port[1])
          if host_port[0] == my_hostname and host_port[1] == str(port):
            my_worker_id = id_cur
          print "Used ", id_cur
          id_cur = id_cur + 1 

    num_workers = id_cur

    print "my worker_id ", my_worker_id
    print "num_workers ", num_workers

    t1 = time.time()
    for i in xrange(0, num_workers):
      if i != my_worker_id:
        d = exampleutils.RandomDataGenerator(bytes_n)
        key_name = 'shuffle_' + str(my_worker_id) + '_' + str(i)
        outf = open(shuffle_dir_name + '/' + key_name, 'w')
        block = d.read(block_size)
        while block != "":
          outf.write(block)
          block = d.read(block_size)
        outf.flush()
        outf.close()

    t2 = time.time()

    write_mb_rate = bytes_n * (num_workers - 1)/(t2-t1)/1e6

    # Read in 
    t3 = time.time()
    bytes_read = 0
    blocks_to_read = list(range(num_workers))
    blocks_to_read.remove(my_worker_id)
    blocks_to_read = deque(blocks_to_read)

    while blocks_to_read:
      m = hashlib.md5()
      block_id = blocks_to_read.pop()
      key = 'shuffle_' + str(block_id) + '_' + str(my_worker_id)
      host_port = hostname_id_map[block_id]
      conn = httplib.HTTPConnection(host_port[0] + ":" + host_port[1])
      conn.request("GET", key)
      r1 = conn.getresponse()
      if r1.status == 200:
        buf = r1.read(block_size)
        while len(buf) > 0:
          bytes_read += len(buf)
          m.update(buf)
          buf = r1.read(block_size)
      else:
        print 'Failed to get ', key, ' status ', r1.status, ' reason ', r1.reason
        blocks_to_read.append(block_id)


    print "Finished reading ", bytes_read
    t4 = time.time()
    read_mb_rate = bytes_read/(t4-t3)/1e6

    res = (t1, t2, write_mb_rate, t3, t4, read_mb_rate)
    print res
    #pickle.dump(res, open(outfile + str(my_worker_id), 'w'))

if __name__ == '__main__':
    cli()

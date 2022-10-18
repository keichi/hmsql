#!/usr/bin/env python3

import json

from dask.distributed import Client, progress
from dask_jobqueue import SGECluster

cluster = SGECluster(cores=24,
                     processes=24,
                     memory="250GB",
                     queue="grid_short.q",
                     interface="ib0",
                     scheduler_options={"interface": "bond1"},
                     local_directory="/var/tmp",
                     job_extra=["-pe smp 24"],
                     walltime="04:00:00")

cluster.scale(jobs=20)

client = Client(cluster)

import time

print("Waiting for cluster to start up")
while sum(client.ncores().values()) < 480:
    time.sleep(1)

print("Running benchmark")
print(client.benchmark_hardware())

client.close()

#!/usr/bin/env python

import os
from os.path import dirname, join
import time

hostfile_name = "machinefiles"
workerfile_name = "workerfiles"
script_dir = dirname(os.path.realpath(__file__))
script_root_dir = join(script_dir, "..", "..")

env_file = join(script_root_dir, "pmls-caffe-env.sh")

hostfile = join(script_dir, hostfile_name)
workerfile = join(script_dir, workerfile_name)

ssh_cmd = (
    "ssh "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null "
    )

# Get host IPs
with open(workerfile, "r") as f:
  hostlines = f.read().splitlines()
host_ips = [line.split()[1] for line in hostlines]

for client_id, ip in enumerate(host_ips):
  cmd = ssh_cmd + ip + " "
  cmd += "\'"
  cmd += "source %d;" % (env_file)
  cmd += " python " + join(script_root_dir, "examples/cifar10/run_local.py")
  cmd += " %d %s %r\'" % (client_id, hostfile, "false")
  cmd += " &"
  print cmd
  os.system(cmd)

  if client_id == 0:
    print "Waiting for first client to set up"
    time.sleep(2)

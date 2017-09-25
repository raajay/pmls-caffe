#!/usr/bin/python
"""Script to monitor if caffe_main is running on the remote hosts.

Terminate only if none of the hosts have caffe_main running.

"""
import time
import subprocess


def is_running(host, proc_name='caffe_main'):
    process = subprocess.Popen(['ssh', host, '-t', 'pgrep -f -a', proc_name],
                               stdout=subprocess.PIPE)
    out, err = process.communicate()
    if proc_name in out:
        return True
    return False


def poll(hosts, interval=60):
    """Polling the remote hosts at regular intervals.

    @param interval: The polling interval

    """
    should_continue = True
    while should_continue:
        should_continue = False
        for h in hosts:
            should_continue = should_continue or is_running(h, 'caffe_main')
        break
        time.sleep(interval)
    return


def main():
    with open('workerfiles') as fp:
        hosts = [line.strip().split(' ')[1]
                 for line in fp.readlines()]

    poll(hosts)
    return


if __name__ == '__main__':
    main()

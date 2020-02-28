import os
import sys
import time
import random

pid = os.getpid()
args = sys.argv[1:]
print(f'[{pid}] Running test.py with args {args}.')
sleeptime = random.randint(1, 15)
print(f'[{pid}] Sleeping for {sleeptime} seconds.')
time.sleep(sleeptime)
print(f'[{pid}] Done.')

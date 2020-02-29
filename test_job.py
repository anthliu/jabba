import os
import sys
import time
import random

pid = os.getpid()
args = sys.argv[1:]
print(f'[{pid}] Running test.py with args {args}.')
sleeptime = random.randint(1, 5)
print(f'[{pid}] Sleeping for {sleeptime} seconds.')
if any('fail' in arg for arg in args):
    raise Exception('[{pid}] Failing on purpose now.')
time.sleep(sleeptime)
print(f'[{pid}] Done.')

import argparse
from datetime import datetime
import json
import itertools
import collections
from enum import Enum
import heapq
from pathlib import Path
import subprocess
import asyncio
import random
import math

Job = collections.namedtuple('Job', ['priority', 'load', 'command', 'log_path', 'config', 'pconfig'])
SampleToken = collections.namedtuple('SampleToken', ['dist', 'a', 'b'])

def token_sample(token):
    if token.dist == 'uniform':
        return random.uniform(token.a, token.b)
    elif token.dist == 'loguniform':
        low = math.log(token.a)
        high = math.log(token.b)
        return math.exp(random.uniform(low, high))
    else:
        raise NameError

def dict_product(d):
    keys = d.keys()
    yield from (
        dict(zip(keys, result))
        for result in itertools.product(*[d[key] for key in keys])
    )

def parse_flag(flag):
    parts = flag.split('.')
    start_index = 0
    while start_index < len(parts):
        if not parts[start_index].startswith('@'):
            break
        start_index += 1
    return parts[:start_index], '.'.join(parts[start_index:])

def parse_jobs(cfg, overrides):
    jobs = []
    global_cfg = cfg.get('@global', {})
    seed = global_cfg.get('@seed', 0)
    seed = overrides.get('@seed', seed)
    random.seed(seed)

    for local_cfg in cfg['@jobs']:
        job_cfg_ = dict(global_cfg)
        job_cfg_.update(local_cfg)
        job_cfg_.update(overrides)

        # Calculate sweep flags
        sweep_flags = {}
        remove_flags = []
        for flag, value in job_cfg_.items():
            remove_flags.append(flag)
            special_parts, sweep_flag = parse_flag(flag)
            if '@sweep' in special_parts:
                sweep_flags[sweep_flag] = value
            elif '@uniform' in special_parts:
                samples = [SampleToken('uniform', value[0], value[1]) for _ in range(value[2])]
                sweep_flags[sweep_flag] = samples
            elif '@loguniform' in special_parts:
                samples = [SampleToken('loguniform', value[0], value[1]) for _ in range(value[2])]
                sweep_flags[sweep_flag] = samples
            else:
                remove_flags.pop()
        for sweep_flag in remove_flags:
            job_cfg_.pop(sweep_flag)

        # create a job for each sweep value
        for sweep in dict_product(sweep_flags):
            for flag in sweep.keys():
                if isinstance(sweep[flag], SampleToken):
                    sweep[flag] = token_sample(sweep[flag])
            job_cfg_.update(sweep)
            job_cfg = dict(job_cfg_)# save job_cfg_ for later

            # deal with references
            flag_to_name = {}
            for flag in job_cfg.keys():
                special_parts, flag_name = parse_flag(flag)
                if len(flag_name) == 0:
                    flag_to_name[flag] = special_parts[-1]
                else:
                    flag_to_name[flag] = '@' + flag_name
            reference_queue = list(job_cfg.keys())
            timeout = 100
            sorted_flags = list(sorted(job_cfg.keys(), key=len, reverse=True))# replace larger vars first (nesting case)
            while len(reference_queue) > 0 and timeout > 0:
                ref_flag = reference_queue.pop(0)
                timeout -= 1
                new_value = old_value = job_cfg[ref_flag]
                if isinstance(new_value, str):
                    for flag in sorted_flags:
                        replace_val = job_cfg[flag]
                        if isinstance(replace_val, float):
                            if 0.01 <= abs(replace_val) < 100 or replace_val == 0.0:
                                replace_with = f'{replace_val:.2f}'
                            else:
                                replace_with = f'{replace_val:.2e}'
                        else:
                            replace_with = str(replace_val)
                        new_value = new_value.replace(flag_to_name[flag], replace_with)
                    if new_value != old_value:
                        reference_queue.append(ref_flag)
                    job_cfg[ref_flag] = new_value
            if timeout <= 0:
                raise Exception('Timeout. Reference loading (probably) encountered loop.')

            # parse magic commands
            assert '@program' in job_cfg
            cmd = job_cfg.pop('@program')
            if '@format' in job_cfg:
                flag_format = job_cfg.pop('@format')
            else:
                flag_format = 'flag'

            load = job_cfg.get('@load', 1)
            priority = job_cfg.get('@priority', 0)
            log_path = job_cfg.get('@log_path', datetime.now().strftime('%Y-%m-%d-%H-%M-%S.log'))

            # format final command
            for flag, value in job_cfg.items():
                special_parts, flag_name = parse_flag(flag)
                if '@env' in special_parts:
                    cmd = f'{flag_name}={value} {cmd}'
                elif '@gin' in special_parts:
                    cmd += f" --gin_param='{flag_name} = {value}'"
                elif len(flag_name) == 0:
                    pass
                elif flag_format == 'flag':
                    cmd += f' --{flag_name} {value}'
                elif flag_format == '=':
                    cmd += f' {flag_name}={value}'
                else:
                    raise Exception(f'Unknown flag_format: {flag_format}.')

            jobs.append(Job(priority, load, cmd, log_path, dict(job_cfg_), dict(job_cfg)))

    return jobs

def run_job(job, dry_run=False):
    if dry_run:
        print(f'Test running process "{job.command}".')
        return 0

    start_time = datetime.now()
    start_time_s = start_time.strftime("%Y-%m-%d (%H:%M:%S)")
    print(f'[{start_time_s}] Running process "{job.command}"')

    try:
        result = subprocess.check_output(job.command, shell=True, text=True)
    except subprocess.CalledProcessError as e:
        end_time = datetime.now()
        end_time_s = end_time.strftime("%Y-%m-%d (%H:%M:%S)")

        #print(f'[{end_time_s}] Process "{job.command}" terminated with ERROR. Finished in {end_time - start_time}')
        #print(e.output)
        raise e

    end_time = datetime.now()
    end_time_s = end_time.strftime("%Y-%m-%d (%H:%M:%S)")

    print(f'[{end_time_s}] Process finished "{job.command}". Finished in {end_time - start_time}')

    return 0

async def run_job_async(job, dry_run=False):
    if dry_run:
        print(f'Test running process "{job.command}".')
        return 0

    start_time = datetime.now()
    start_time_s = start_time.strftime("%Y-%m-%d (%H:%M:%S)")
    print(f'[{start_time_s}] Running process "{job.command}"')
    if job.log_path != 'stdout':
        with open(job.log_path, 'w') as f:
            proc = await asyncio.create_subprocess_shell(job.command, stdout=f, stderr=asyncio.subprocess.PIPE)
    else:
        proc = await asyncio.create_subprocess_shell(job.command, stderr=asyncio.subprocess.PIPE)
    result = await proc.wait()
    end_time = datetime.now()
    end_time_s = end_time.strftime("%Y-%m-%d (%H:%M:%S)")
    if result != 0:
        stderr = await proc.stderr.read()
        print(f'[{end_time_s}] Process "{job.command}" terminated with ERROR code {result}. Finished in {end_time - start_time}')
        print(stderr.decode('utf-8'))
    else:
        print(f'[{end_time_s}] Process finished "{job.command}". Finished in {end_time - start_time}')

    return result


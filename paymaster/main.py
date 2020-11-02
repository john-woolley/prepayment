import os
from datetime import datetime
from dask.distributed import Client, performance_report
from paymaster.dask_config.environment import DaskEnv
from paymaster.pipelines import ingestion
from paymaster.stdout.formatting import load_title

VERSION = '0.1a'
APP_NAME = 'PAYMASTER'
DATE = '2020-11-01'
REPORT_ROOT = '/var/www/html/paymaster-reports/'
LOG_ROOT = '/var/log/paymaster/'
if __name__ == '__main__':
    __spec__ = None
    load_title(APP_NAME, VERSION, DATE)
    print(datetime.now())
    cluster_resume = '192.168.2.5:8786'
    if cluster_resume != 'new':
        print(cluster_resume)
        client = Client(cluster_resume)
        print(client)
        env = DaskEnv(client=client)
    else:
        mem_limit = str(input('Input Max Ram (GB) [Default=8]')
                        or '8')
        procs = str(input('Input Number of Processes [Default=1]')
                        or '1')
        threads = str(input('Input Threads per Process [Default=2]')
                      or '2')
        env = DaskEnv(mem_limit=mem_limit, nprocs=procs, nthreads=threads)
    jobs = [ingestion.to_parquet]
    schema = 'schema/perf_schema.json'
    schema_name = 'perf'
    ingest_dir = '/mnt/data/fnma-data/sf/perf/raw'
    files = os.listdir(ingest_dir)
    mods = [job.__module__.split('.') for job in jobs]
    names = [job.__name__ for job in jobs]
    modnames = ['.'.join([mods[i], names[i]]) for i in range(len(mods))]
    print('{client} \n queuing {jobs} in {files}'.format(
        client=repr(env.client), jobs=jobs, files=files))
    ts = datetime.now().strftime('%Y%m%d%H%M%S')
    logfile = LOG_ROOT + '{}-{}.log'.format(__name__, ts)
    fn = ('dark-performance-report-{}-{}-{}.html'
          .format(__name__, schema_name, ts))
    report_path = REPORT_ROOT + fn
    with performance_report(report_path):
        try:
            for job in jobs:
                res = job(logfile)
        except Exception as e:
            env.shutdown()
            raise e

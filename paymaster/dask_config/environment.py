import dask
from dask.distributed import Client, LocalCluster, SpecCluster

dask.config.set(temporary_directory='/tmp/')

CLUSTER_TYPES = {'local': LocalCluster, 'spec': SpecCluster}

class DaskEnv:
    def __init__(self,
                 client: Client = None,
                 cluster_type: str = 'local',
                 scheduler = None, # {'address': 'tcp://192.168.2.5:8786'}
                 workers = None,
                 mem_limit: str = '8',
                 nprocs: str = '2',
                 nthreads: str = '2',
                 webdriver: int = 0,
                 webdriver_path: str = None,
                 user: str = None,
                 conda_name: str = None
                 ):

        self.client = client
        if self.client:
            if self.client.cluster:
                self.cluster = self.client.cluster
        else:
            self.cluster_type = cluster_type
            self.scheduler = scheduler
            self.host = self.scheduler['address'].split(':')[0]
            self.workers = workers
            self.user = user
            self.conda_name = conda_name
        self.mem_limit = mem_limit
        self.nprocs = nprocs
        self.nthreads = nthreads
        self.webdriver = webdriver
        self.webdriver_path = webdriver_path
        if not self.client:
            self.connect()

    def connect(self):
        self.connect_dask()
        if self.webdriver:
            self.connect_webdriver()

    def connect_dask(self):
        klass = CLUSTER_TYPES[self.cluster_type]
        if klass == LocalCluster:
            try:
                client = Client(host=self.scheduler['address'])
                self.client = client
            except OSError:
                print('oserror')
                cluster = klass(self.scheduler['address'])
                client = Client(cluster)
                self.client = client
                self.cluster = cluster
        elif klass == SpecCluster:
            raise NotImplemented
            # p = subprocess.Popen("{cmd}".format(
            #   cmd='. ~/.bashrc conda activate {}; '
            #   'dask-scheduler'.format(self.conda_name)),
            #   shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # out, error = p.communicate()
            # print(out, error)
            # for line in p.stdout: print(line.decode(), end='')
            # p.stdout.close()
            # return_code = p.wait()

                # for w in self.workers:
            #     p = subprocess.Popen(
            #     "{cmd}".format(
            #       cmd='. ~/.bashrc conda activate {}'
            #       .format(self.conda_name)),
            #       shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            #     out, err = p.communicate()

    def connect_webdriver(self):
        raise NotImplemented

    def shutdown(self, cluster:bool=False):
        self.client.shutdown()
        self.client.close()
        if cluster:
            try:
                self.cluster.close()
            except AttributeError:
                pass

if __name__ == '__main__':
    test_case_client = DaskEnv(client=Client('constantinopolis:8786'))
    print(test_case_client.client)
    # test_case_spec = DaskEnv(cluster_type='spec',
    # user='augustus', scheduler={'address':'constantinopolis:40859',},
    #                     conda_name='prepayment')

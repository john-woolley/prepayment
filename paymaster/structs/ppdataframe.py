import json
import yaml
import dask.dataframe as dd
import os
import psutil
import pyarrow as pa


class PrepayDataFrame:
    def __init__(self,
                 schema_json: str = None,
                 schema_yaml: str = None
                 ):

        if schema_json:
            self.schema = json.load(open(schema_json))
        elif schema_yaml:
            self.schema = yaml.load(open(schema_yaml))
        self.raw_src = self.schema['raw_source']
        self.parquet_src = self.schema['parquet_source']
        self.parquet_tgt = self.schema['parquet_target']
        ingest = self.schema['ingest_columns']
        self.column_headers = {v['idx']: k
                               for k, v in ingest.items()}
        self._arg_cols = []
        self._get_arg_cols()
        self.date_columns = {}
        self._get_date_cols()
        self._dropped_cols = []
        self._get_dropped_cols()
        self.dd_types = {}
        self._get_types()
        self.pa_types = {k: getattr(pa, v['pa_type'])()
                         for k, v in ingest.items()
                         if k not in self._dropped_cols}
        self._categories = []

        self.data = dd.DataFrame = None
        self.ingested_files = 0

    def _get_arg_cols(self):
        for k, v in self.schema['ingest_columns'].items():
            if 'args' in v:
                self._arg_cols.append(k)

    def _get_dropped_cols(self):
        for k, v in self.schema['ingest_columns'].items():
            if k in self._arg_cols:
                if 'drop' in v['args']:
                    self._dropped_cols.append(k)

    def _get_date_cols(self):
        for k, v in self.schema['ingest_columns'].items():
            if k in self._arg_cols:
                if 'date' in v['args']:
                    self.date_columns[v['idx']] = v['args']['date']

    def _get_categories(self):
        for k, v in self.schema['ingest_columns'].items():
            if k in self._arg_cols:
                if 'category' in v['args']:
                    self._categories.append(k)

    def _get_types(self):
        for k, v in self.schema['ingest_columns'].items():
            if k not in self._dropped_cols:
                if '.' in v['dd_type']:
                    pkg, typ = v['dd_type'].split('.')
                    self.dd_types[v['idx']] = getattr(globals()[pkg], typ)()
                else:
                    self.dd_types[v['idx']] = v['dd_type']

    def read_csv(self):
        self.ingested_files = os.listdir(self.raw_src)
        data = dd.read_csv(self.raw_src+'/*', sep='|', header=None,
                           dtype=self.dd_types, parse_dates=self.date_columns)
        data = data.rename(columns=self.column_headers)
        data = data.drop(columns=self._dropped_cols)
        data = data.categorize(self._categories)
        self.data = data

    def read_parquet(self):
        self.ingested_files = os.listdir(self.parquet_src)
        self.data = dd.read_parquet(self.parquet_src, engine='pyarrow')

    def to_parquet(self, path: str = None, infer_schema: bool = False):
        if not path:
            path = self.parquet_tgt
        if not infer_schema:
            self.data.to_parquet(path=path, engine='pyarrow',
                                 schema=self.pa_types)
        else:
            self.data.to_parquet(path=path, engine='pyarrow', schema='infer')

    def set_index(self, col: str, drop=True):
        self.data = self.data.set_index(col, drop=drop)

import json
import dask.dataframe as dd
import os
import pyarrow as pa

class PrepayDataFrame:
    def __init__(self, schema_json: str ='schema/perf_schema.json'):
        self.schema = json.load(open(schema_json))
        self.raw_src = self.schema['raw_source']
        self.parquet_src = self.schema['parquet_source']
        self.parquet_tgt = self.schema['parquet_target']
        self.dd_types = {v['column_idx']:v['dd_type']
            for v in self.schema['columns'].values()}
        self.pa_types = {k:(getattr(pa, v['pa_type']))
            for k, v in self.schema['columns'].items()}
        self.column_headers = {v['column_idx']:k
            for k,v in self.schema['columns'].items()}
        self.date_columns = [v['column_idx']
            for v in self.schema['columns'].values if v['date_column']]
        self.data = dd.DataFrame = None
        self.ingested_files = 0

    def read_csv(self):
        self.ingested_files = os.listdir(self.raw_src)
        data = dd.read_csv(self.raw_src+'/*', sep='|', header=None,
                    dtype=self.dd_types, parse_dates=self.date_columns)
        data = data.rename(columns=self.column_headers)
        self.data = data

    def read_parquet(self):
        self.ingested_files = os.listdir(self.parquet_src)
        self.data = dd.read_parquet(self.parquet_src, engine='pyarrow')

    def to_parquet(self, path=None, infer_schema=False):
        if not path:
            path = self.parquet_tgt
        if not infer_schema:
            self.data.to_parquet(path=path, engine='pyarrow',
                                 schema=self.pa_types)
        else:
            self.data.to_parquet(path=path, engine='pyarrow', schema='infer')

    def set_index(self, col:str, drop=True):
        self.data = self.data.set_index(col, drop=drop)

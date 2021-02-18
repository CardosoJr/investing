import os
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


class file_handler:
    '''
    class for handling directory/folder creation + data saving

    Attributes
        project_dir : str(), main project directory

    Methods
        save_data : actual public save function
            |__> _create_dir : internal fn to create dir if it does not exist
                |_> __check_exists_or_create : private fn to check if file exists
            |__> __create_date_str : private fn to create date str
            |__> __create_timestamp_str : private fn to create timestamp str

    '''
    def __init__(self, project_dir, mode):
        self.project_dir = project_dir
        self.mode = mode

    def __check_exists_or_create(self, _dir):
        """fn: to check if file/path exists"""
        if not Path(_dir).exists():
            try:
                Path(_dir).mkdir(parents = True)
            except Exception as e:
                print(e)
        return

    def _create_dir(self, grouping):
        """fn: create daily directory if not already created"""
        _dir = self.project_dir+'/' + self.mode + '/' + grouping +'/'
        self.__check_exists_or_create(_dir)
        return _dir

    def __append_data(self, data, file_path, format):
        if format == "parquet":
            old_data = pq.read_table(file_path).to_pandas()
        elif format == "h5":
            old_data = pd.read_hdf(file_path, key="data")
        elif format == "csv":
            old_data = pd.read_csv(file_path)
        elif format == "feather":
            old_data = pd.read_feather(file_path)
        else:
            raise Exception("Format not found")
        return data.append(old_data)

    def save_data(self, data, timestamp, group, format='parquet', errors=False):
        _dir = Path(self._create_dir(group))
        _timestamp = timestamp

        if errors:
            _fp = _dir / f'{self.mode}_errors_{_timestamp}.{format}'
        else:
            _fp = _dir / f'{self.mode}_data_{_timestamp}.{format}'

        if _fp.exists:
            data = self.__append_data(data, _fp, format)

        if format=='parquet':
            _table = pa.Table.from_pandas(data)
            pq.write_table(_table, _fp)

        elif format == 'h5': data.to_hdf(_fp, key='data')
        elif format == 'csv': data.to_csv(_fp, index=False)
        elif format == 'feather': data.to_feather(_fp)
        else: raise Exception("Format not found")
        return
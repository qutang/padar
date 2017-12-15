import os
import glob
import re
import pandas as pd
import numpy as np
from .utils import *
from pathos.multiprocessing import ProcessingPool as Pool
from multiprocessing import cpu_count
from functools import partial

class M:
    """[summary]
    
    [description]
    """
    
    def __init__(self, root):
        self._root = str.strip(root)
        self._summary_funcs = {
            'file_size': lambda x: os.path.getsize(x) / 1024 / 1024.0,
            'sensor_stat': sensor_stat
        }
        self._num_of_cpu = cpu_count()

    def summarize(self, use_parallel=False, verbose=False):
        result = self.scan(self._summary_funcs, use_parallel=use_parallel, verbose=verbose)
        self._summary = result
        return self

    def summarize_partial(self, rel_path, use_parallel=False, verbose=False):
        result = self._scan_use_glob(os.path.join(self._root, rel_path), self._summary_funcs, use_parallel=use_parallel, verbose=verbose)
        # result = self._scan(os.path.join(self._root, rel_path), self._summary_funcs, verbose=verbose)
        result.insert(0, 'path', rel_path)
        result = result.sort_values(by = ['path', 'id', 'type', 'date', 'hour']).reset_index(drop=True)
        return result
  
    def scan(self, func_dict, use_parallel=False, verbose=False):
        df = pd.DataFrame()
        for p in self.participants:
            if verbose:
                print('processing ' + p)
            p_df = self._scan_use_glob(self._root + '/' + p, func_dict, use_parallel=use_parallel, verbose=verbose)
            # p_df = self._scan(self._root + '/' + p, func_dict, verbose=verbose)
            p_df.insert(0, 'pid', p)
            df = df.append(p_df, ignore_index=True)
        result = df.sort_values(by = ['pid', 'id', 'type', 'date', 'hour']).reset_index(drop=True)
        return df

    def _process_file(self, file, func_dict, verbose=False):
        if verbose:
                print('processing ' + file)
        row = {}
        keys = []
        extra_dfs = []
        for name, func in func_dict.items():
            result = func(file)
            if type(result) is not pd.DataFrame:
                row[name] = result
                keys.append(name)
            else:
                extra_dfs.append(result)
        print(file)
        row['date'] = extract_date(file)
        row['hour'] = extract_hour(file)
        row['type'] = extract_file_type(file)
        row['id'] = extract_id(file)
        row_df = pd.DataFrame(data=row, index=[0])
        row_df = row_df[['id', 'type', 'date', 'hour'] + keys]
        row_df = pd.concat([row_df] + extra_dfs, axis=1)
        return row_df

    def _scan_use_glob(self, folder, func_dict, use_parallel=False, verbose=False):
        

        entry_files = glob.glob(os.path.join(folder, '**', '*.csv*'),recursive=True)
        # TODO: parallel version
        if use_parallel:
            pool = Pool(self._num_of_cpu)
            # _process_file_partial = partial(_process_file, verbose)
            df = pd.concat(pool.map(self._process_file, entry_files, [func_dict] * len(entry_files)))
            pool.close()
        else:
            df = pd.DataFrame()
            for file in entry_files:
                df = df.append(self._process_file(file, func_dict, verbose=verbose), ignore_index=True)
        return df

    def _scan(self, folder, func_dict, verbose=False):
        df = pd.DataFrame()
        for entry in os.scandir(folder):
            if entry.is_file() and not self._excluded_files(os.path.basename(entry.path)):
                if verbose:
                    print('processing ' + entry.path)
                row = {}
                keys = []
                extra_dfs = []
                for name, func in func_dict.items():
                    result = func(entry.path)
                    if type(result) is not pd.DataFrame:
                        row[name] = result
                        keys.append(name)
                    else:
                        extra_dfs.append(result)
                row['date'] = self._extract_date(entry.path)
                row['hour'] = self._extract_hour(entry.path)
                row['type'] = self._extract_file_type(entry.path)
                row['id'] = self._extract_id(entry.path)
                row_df = pd.DataFrame(data=row, index=[0])
                row_df = row_df[['id', 'type', 'date', 'hour'] + keys]
                row_df = pd.concat([row_df] + extra_dfs, axis=1)
                df = df.append(row_df, ignore_index=True)
            elif entry.is_dir():
                df = df.append(self._scan(entry.path, func_dict, verbose=verbose), ignore_index=True)
        return df

    @property
    def summary(self):
        return self._summary

    @property
    def participants(self):
        """[summary]
        
        [description]
        
        Returns:
            [type] -- [description]
        """
        return self._get_participants()
    
    def sensors(self, pid):
        pid_folder = self._root + '/' + pid
        return self._get_sensors(pid_folder)

    def annotators(self, pid):
        pid_folder = self._root + '/' + pid
        return self._get_annotators(pid_folder)

    def folder_size(self, pid):
        total = 0
        for entry in os.scandir(self._root + '/' + pid):
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += self.folder_size(entry.path)
        return total

    def total_size(self, pattern):
        selected_files = glob.glob(os.path.join(self._root, pattern), recursive=True)
        sizes = [os.path.getsize(file) for file in selected_files]
        return np.sum(sizes)

    def file_sizes(self, pattern, by):
        selected_files = glob.glob(os.path.join(self._root, pattern), recursive=True)
        hours = [self._extract_hour(file) for file in selected_files]
        dates = [self._extract_date(file) for file in selected_files]
        sizes = [os.path.getsize(file) for file in selected_files]
        size_df = pd.DataFrame({"date": dates, "hour": hours, "size": sizes}, index=None)
        if by == 'day':
            day_df = size_df.groupby(by='date')['size'].sum()
            day_df = day_df.reset_index()
            return day_df
        elif by == 'hour':
            return size_df
        else:
            raise ValueError('Unrecognized argument by: ' + by)

    def _get_annotators(self, folder):
        annotation_files = glob.glob(folder + "/**/*.annotation.csv", recursive=True)
        return set([self._extract_annotators(file) for file in annotation_files])
        
    def _get_sensors(self, folder):
        sensor_files = glob.glob(folder + "//**/*.sensor.csv", recursive=True)
        return set([self._extract_sensor_id(file) for file in sensor_files])

    def _extract_date(self, file):
        return re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}', os.path.basename(file).split('.')[2]).group(0)

    def _extract_hour(self, file):
        return re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}-([0-9]{2})', os.path.basename(file).split('.')[2]).group(1)

    def _extract_id(self, sensor_file):
        return os.path.basename(sensor_file).split('.')[1].split('-')[0].upper().strip()

    def _extract_file_type(self, file):
        return os.path.basename(file).split('.')[3].lower().strip()

    def _get_participants(self):
        return [name for name in os.listdir(self._root) if os.path.isdir(os.path.join(self._root, name)) and not self._excluded_files(name)]

    def _excluded_files(self, name):
        exclude = False
        exclude = exclude or name == '.git'
        exclude = exclude or name == '.DS_Store'
        return exclude
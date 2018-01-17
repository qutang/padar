import os
import glob
import re
import pandas as pd
import numpy as np
from pathos.multiprocessing import ProcessingPool as Pool
from multiprocessing import cpu_count
from functools import partial
from .utils import *

class M:
    """[summary]
    
    [description]
    """
    
    def __init__(self, root):
        self._root = str.strip(root)
        self._summary_funcs = {
            'file_size': lambda x: os.path.getsize(x) / 1024.0,
            # 'exists': lambda x: os.path.exists(x)
            # 'sensor_stat': sensor_stat
        }
        self._num_of_cpu = cpu_count()

    def summarize(self, rel_path = "", use_parallel=False, verbose=False):
        if use_parallel:
            self._pool = Pool(self._num_of_cpu - 1)
        if rel_path == "":
            rel_path = os.path.join(self._root, "*", "MasterSynced")
        else:
            rel_path = os.path.join(self._root, rel_path)
        
        result = self._summarize(rel_path, self._summary_funcs, use_parallel=use_parallel, verbose=verbose)
        if use_parallel:
            self._pool.close()
        return result
  
    def _summarize(self, folder, func_dict, use_parallel=False, verbose=False):
        entry_files = glob.glob(os.path.join(folder,'**', '*.csv*'), recursive=True)
        # parallel version
        if use_parallel:
            df = pd.concat(self._pool.map(self._summarize_file, entry_files, [func_dict] * len(entry_files), [verbose] * len(entry_files)))
        else:
            df = pd.DataFrame()
            for file in entry_files:
                df = df.append(self._summarize_file(file, func_dict, verbose=verbose), ignore_index=True)
        df = df.sort_values(by = ['pid', 'id', 'type', 'date', 'hour']).reset_index(drop=True)
        return df

    def _summarize_file(self, file, func_dict, verbose=False):
        file = os.path.abspath(file)
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
        row['date'] = extract_date(file)
        row['hour'] = extract_hour(file)
        row['type'] = extract_file_type(file)
        row['id'] = extract_id(file)
        row['pid'] = extract_pid(file)
        row_df = pd.DataFrame(data=row, index=[0])
        row_df = row_df[['pid', 'id', 'type', 'date', 'hour'] + keys]
        row_df = pd.concat([row_df] + extra_dfs, axis=1)
        return row_df

    def process(self, rel_pattern = "", func=None, use_parallel=False, verbose=False, **kwargs):
        if use_parallel:
            self._pool = Pool(self._num_of_cpu - 1)
        if rel_pattern == "":
            rel_path = os.path.join(self._root, "*", "MasterSynced")
        else:
            rel_path = os.path.join(self._root, rel_pattern)
        result = self._process(rel_path, func, use_parallel=use_parallel, verbose=verbose, **kwargs)
        if use_parallel:
            self._pool.close()
        return result

    def _process(self, pattern, func, use_parallel=False, verbose=False, **kwargs):
        if func is None:
            raise ValueError("You must provide a function to process files")
        entry_files = glob.glob(pattern, recursive=True)
        prev_files = self._get_prev_files(entry_files)
        next_files = self._get_next_files(entry_files)

        # parallel version
        if use_parallel:
            def zipped_func(a_zip, verbose=False, **kwargs):
                return func(a_zip[0], verbose=verbose, prev_file=a_zip[1], next_file=a_zip[2], **kwargs)
            
            func_partial = partial(zipped_func, verbose=verbose, **kwargs)
            result = self._pool.map(func_partial, zip(entry_files, prev_files, next_files))
        else:
            result = []
            for file, prev_file, next_file in zip(entry_files, prev_files, next_files):
                entry_result = func(file, verbose=verbose, prev_file=prev_file, next_file=next_file, **kwargs)
                result.append(entry_result)
        result = pd.concat(result) 
        return result

    def _get_prev_files(self, entry_files):
        entry_files = np.array(entry_files)
        prev_files = np.copy(entry_files)
        prev_files = np.roll(prev_files, 1)
        prev_files[0] = None
        entry_pids = np.array(list(map(lambda name: extract_pid(name), entry_files)))
        prev_pids = np.array(list(map(lambda name: extract_pid(name), prev_files)))
        make_none_mask = entry_pids != prev_pids
        prev_files[make_none_mask] = None
        return prev_files.tolist()

    def _get_next_files(self, entry_files):
        entry_files = np.array(entry_files)
        next_files = np.copy(entry_files)
        next_files = np.roll(next_files, -1)
        next_files[-1] = None
        
        entry_pids =  np.array(list(map(lambda name: extract_pid(name), entry_files)))
        next_pids = np.array(list(map(lambda name: extract_pid(name), next_files)))
        make_none_mask = entry_pids != next_pids
        next_files[make_none_mask] = None
        return next_files.tolist()

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

    def _get_annotators(self, folder):
        annotation_files = glob.glob(folder + "/**/*.annotation.csv", recursive=True)
        return set([self._extract_annotators(file) for file in annotation_files])
        
    def _get_sensors(self, folder):
        sensor_files = glob.glob(folder + "//**/*.sensor.csv", recursive=True)
        return set([self._extract_sensor_id(file) for file in sensor_files])

    def _get_participants(self):
        return [name for name in os.listdir(self._root) if os.path.isdir(os.path.join(self._root, name)) and not self._excluded_files(name)]

    def _excluded_files(self, name):
        exclude = False
        exclude = exclude or name == '.git'
        exclude = exclude or name == '.DS_Store'
        exclude = exclude or name == '.vscode'
        exclude = exclude or name == 'DerivedCrossParticipants'
        exclude = exclude or name == 'src'
        exclude = exclude or name == '__pycache__'
        return exclude
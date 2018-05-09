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
            'num_of_rows': num_of_rows,
            'mh_folder_structure': validate_folder_structure,
            'mh_filename': validate_filename,
            'csv_header': validate_csv_header,
            'na_rows': na_rows
        }
        self._num_of_cpu = cpu_count()

    def get_root(self):
        return self._root

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
        row['sensortype'] = extract_sensortype(file)
        row['datatype'] = extract_datatype(file)
        row_df = pd.DataFrame(data=row, index=[0])
        row_df = row_df[['pid', 'id', 'type', 'date', 'hour', 'sensortype', 'datatype'] + keys]
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

    def _process(self, pattern, func, use_parallel=False, verbose=False, violate=False, **kwargs):
        if func is None:
            raise ValueError("You must provide a function to process files")
        entry_files = np.array(glob.glob(pattern, recursive=True))
        # sort by pid, sid, date, hour
        if violate == False:
            pids = np.array(list(map(lambda file: extract_pid(file), entry_files)))
            sids = np.array(list(map(lambda file: extract_id(file), entry_files)))
            dates = np.array(list(map(lambda file: extract_date(file), entry_files)))
            hours = np.array(list(map(lambda file: extract_hour(file), entry_files)))
            sorted_inds = np.lexsort((hours, dates, sids, pids)).tolist()
            pids = pids[sorted_inds].tolist()
            sids = sids[sorted_inds].tolist()
            dates = dates[sorted_inds].tolist()
            hours = hours[sorted_inds].tolist()
            prev_files = self._get_prev_files(entry_files, pids, sids)
            next_files = self._get_next_files(entry_files, pids, sids)
            entry_files = entry_files[sorted_inds].tolist()
        else:
            prev_files = [None] * len(entry_files)
            next_files = [None] * len(entry_files)
            sids = ["unknown"] * len(entry_files)
            pids = np.array(list(map(lambda file: extract_pid(file), entry_files)))
            dates = ['unknown'] * len(entry_files)
            hours = ['unknown'] * len(entry_files)
        
        # parallel version
        if use_parallel:
            # def zipped_func(a_zip, verbose=False, **kwargs):
            #     return func(a_zip[0], verbose=verbose, prev_file=a_zip[1], next_file=a_zip[2], **kwargs)
            
            def zipped_func(a_zip):
                return func(verbose=verbose, violate=violate, **kwargs)(a_zip[0], prev_file=a_zip[1], next_file=a_zip[2])

            # func_partial = partial(zipped_func, verbose=verbose, **kwargs)
            # result = self._pool.map(func_partial, zip(entry_files, prev_files, next_files))
            result = self._pool.map(zipped_func, zip(entry_files, prev_files, next_files))
            col_order = []
            
            for entry in result:
                if len(entry.columns) > len(col_order):
                    col_order = entry.columns
        else:
            result = []
            col_order = []
            for file, prev_file, next_file in zip(entry_files, prev_files, next_files):
                # entry_result = func(file, verbose=verbose, prev_file=prev_file, next_file=next_file, **kwargs)
                entry_result = func(verbose=verbose, violate=violate, **kwargs)(file, prev_file=prev_file, next_file=next_file)
                result.append(entry_result)
                if len(entry_result.columns) > len(col_order):
                    col_order = entry_result.columns
        result = pd.concat(result, ignore_index=True)
        result = result[col_order]
        # sort timestamp
        if isinstance(result.iloc[0,0], pd.Timestamp):
            result = result.sort_values(by=result.columns[0])
        return result

    def _get_prev_files(self, entry_files, pids, sids):
        entry_files = np.array(entry_files)
        prev_files = np.copy(entry_files)
        prev_files = np.roll(prev_files, 1)
        prev_files[0] = None
        prev_pids = np.copy(pids)
        prev_pids = np.roll(prev_pids, 1)
        prev_pids[0] = None
        prev_sids = np.copy(sids)
        prev_sids = np.roll(prev_sids, 1)
        prev_sids[0] = None
        make_none_mask = (pids != prev_pids) | (sids != prev_sids)
        prev_files[make_none_mask] = None
        return prev_files.tolist()

    def _get_next_files(self, entry_files, pids, sids):
        entry_files = np.array(entry_files)
        next_files = np.copy(entry_files)
        next_files = np.roll(next_files, -1)
        next_files[-1] = None
        next_pids = np.roll(pids, -1)
        next_pids[-1] = None
        next_sids = np.roll(sids, -1)
        next_sids[-1] = None
        make_none_mask = (pids != next_pids) | (sids != next_sids)
        next_files[make_none_mask] = None
        return next_files.tolist()

    @property
    def participants(self):
        return [name for name in os.listdir(self._root) if os.path.isdir(os.path.join(self._root, name)) and not self._excluded_files(name)]
    
    def sensors(self, pid):
        pid_folder = self._root + '/' + pid
        sensor_files = glob.glob(pid_folder + "//**/*.sensor.csv", recursive=True)
        return set(map(extract_id, sensor_files))

    def annotators(self, pid):
        pid_folder = self._root + '/' + pid
        annotation_files = glob.glob(pid_folder + "/**/*.annotation.csv", recursive=True)
        return set(map(extract_id, annotation_files))   

    def folder_size(self, pid):
        total = 0
        for entry in os.scandir(self._root + '/' + pid):
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += self.folder_size(entry.path)
        return total

    def _excluded_files(self, name):
        exclude = False
        exclude = exclude or name == '.git'
        exclude = exclude or name == '.DS_Store'
        exclude = exclude or name == '.vscode'
        exclude = exclude or name == 'DerivedCrossParticipants'
        exclude = exclude or name == 'src'
        exclude = exclude or name == '__pycache__'
        exclude = exclude or name == '.ipynb_checkpoints'
        return exclude
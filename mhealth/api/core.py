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
        entry_files = np.array(glob.glob(pattern, recursive=True))

        # sort by pid, sid, date, hour
        pids = np.array(list(map(lambda file: extract_pid(file), entry_files)))
        sids = np.array(list(map(lambda file: extract_id(file), entry_files)))
        dates = np.array(list(map(lambda file: extract_date(file), entry_files)))
        hours = np.array(list(map(lambda file: extract_hour(file), entry_files)))
        sorted_inds = np.lexsort((hours, dates, sids, pids)).tolist()
        entry_files = entry_files[sorted_inds].tolist()
        pids = pids[sorted_inds].tolist()
        sids = sids[sorted_inds].tolist()
        dates = dates[sorted_inds].tolist()
        hours = hours[sorted_inds].tolist()
        prev_files = self._get_prev_files(entry_files, pids, sids)
        next_files = self._get_next_files(entry_files, pids, sids)
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

    def merged_subject_meta(self):
        subject_files = self._get_subject_meta_files()
        subject_dfs = []
        for f in subject_files:
            if os.path.exists(f):
                subject_df = pd.read_csv(f)
                subject_dfs.append(subject_df)
        merged_subject_meta = pd.concat(subject_dfs)
        return merged_subject_meta

    def merged_location_mapping(self, filename="Sensor_location.csv"):
        location_files = self._get_meta_files(filename)
        location_dfs = []
        for f in location_files:
            if os.path.exists(f):
                location_df = pd.read_csv(f)
                location_df['PID'] = os.path.basename(os.path.dirname(f))
                location_dfs.append(location_df)
        merged_location_mapping = pd.concat(location_dfs)
        return merged_location_mapping

    def merged_session_meta(self, filename='Sessions.csv'):
        session_files = self._get_meta_files(filename)
        session_dfs = []
        for f in session_files:
            if os.path.exists(f):
                session_df = pd.read_csv(f)
                session_df['PID'] = os.path.basename(os.path.dirname(f))
                session_dfs.append(session_df)
        merged_session_meta = pd.concat(session_dfs)
        return merged_session_meta
        
    def _get_subject_meta_files(self):
        participants = self._get_participants()
        subject_metas = list(map(lambda p: self._root + os.path.sep + p + os.path.sep + 'Subject.csv', participants))
        return subject_metas

    def _get_meta_files(self, filename):
        participants = self._get_participants()
        metas = list(map(lambda p: self._root + os.path.sep + p + os.path.sep + filename, participants))
        return metas

    def _excluded_files(self, name):
        exclude = False
        exclude = exclude or name == '.git'
        exclude = exclude or name == '.DS_Store'
        exclude = exclude or name == '.vscode'
        exclude = exclude or name == 'DerivedCrossParticipants'
        exclude = exclude or name == 'src'
        exclude = exclude or name == '__pycache__'
        return exclude
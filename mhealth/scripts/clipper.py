"""
Script to run on a sensor data file for clipping
"""

import os
import pandas as pd
import numpy as np
import mhealth.api as mh

def main(file, verbose=True, session_file=None, start_time=None, stop_time=None, **kwargs):
    """[summary]
    
    Arguments:
        file {str} -- [description]
        **kwargs {[type]} -- [description]
    
    Keyword Arguments:
        verbose {boolean} -- [description] (default: {True})
        session_file {str} -- [description] (default: {None})
            session_file should have following format:
            START_TIME,STOP_TIME,pid,date,hour
            ...
            timestamp format should be %Y-%m-%d %H:%M:%S
        
        start_time {str, pd.Timestamp} -- [description] (default: {None})
        stop_time {str, pd.Timestamp} -- [description] (default: {None})
    
    Returns:
        [pd.DataFrame] -- [description]
    """

    file = os.path.abspath(file)
    df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
    pid = mh.extract_pid(file)
    clipped = run_clipper(
        df, verbose=verbose, session_file=session_file, start_time=start_time, stop_time=stop_time, pid=pid)
    clipped['pid'] = mh.extract_pid(file)
    clipped['id'] = mh.extract_id(file)
    clipped['date'] = mh.extract_date(file)
    clipped['hour'] = mh.extract_hour(file)
    return clipped

def run_clipper(df, verbose=True, session_file=None, start_time=None, stop_time=None, pid=None, **kwargs):
    if start_time is None and stop_time is None:
        if session_file is not None and pid is not None:
            session_file = os.path.abspath(session_file)
            session_df = pd.read_csv(session_file, parse_dates=[0, 1], infer_datetime_format=True)
            selected_sessions = session_df.loc[session_df['pid'] == pid, :]
            if selected_sessions.shape[0] == 0:
                start_time = None
                stop_time = None
            else:
                start_time = selected_sessions.iloc[0, 0]
                stop_time = selected_sessions.iloc[selected_sessions.shape[0] - 1, 1]
    
    if start_time is not None:
        if type(start_time) is str:
            st = pd.to_datetime(
                start_time, infer_datetime_format=True).to_datetime64().astype(
                    'datetime64[ms]')
        elif type(start_time) is pd.Timestamp:
            st = start_time.to_datetime64().astype('datetime64[ms]')
        else:
            raise ValueError("Unknown timestamp type: " + str(type(start_time)))
    else:
        st = start_time
    if stop_time is not None:
        if type(stop_time) is str:
            et = pd.to_datetime(
                stop_time, infer_datetime_format=True).to_datetime64().astype(
                    'datetime64[ms]')
        elif type(start_time) is pd.Timestamp:
            et = stop_time.to_datetime64().astype('datetime64[ms]')
        else:
            raise ValueError("Unknown timestamp type: " + str(type(stop_time)))
    else:
        et = stop_time
    clipped = mh.clip_dataframe(df, start_time=st, stop_time=et)
    return clipped


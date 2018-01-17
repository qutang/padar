"""
Script to interpolate sensor files automatically. Can handle large gaps in data samples (set by gap_threshold, default is 1 second)

Usage:
    Production:
        Whole dataset:
            `mh -r . process --verbose --par --pattern SPADES_*/MasterSynced/**/Actigraph*.sensor.csv interpolate_sensor --session_file DerivedCrossParticipants/sessions.csv --sr 80`
        Single participant:
            `mh -r . -p SPADES_1 process --par --pattern MasterSynced/**/Actigraph*.sensor.csv interpolate_sensor --session_file DerivedCrossParticipants/sessions.csv --sr 80`
    Debug: 
        `mh -r . -p SPADES_1 process --verbose --pattern MasterSynced/**/Actigraph*.sensor.csv interpolate_sensor --session_file DerivedCrossParticipants/sessions.csv --sr 80`
"""

import os
import pandas as pd
from mhealth.api.interpolate import interpolate
import mhealth.api.utils as utils

def main(file, verbose=True, prev_file=None, next_file=None, session_file=None, sr=None, gap_threshold = 1, **kwargs):
    file = os.path.normpath(os.path.abspath(file))
    df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
    print(df.iloc[0,0])

    if verbose:
        print("Process " + file)
        print("Prev file: " + prev_file)
        print("Next file: " + next_file)

    if sr is None:
        if verbose:
            print("Warning: sampling rate is not set, interpolation will be skipped and original data will be saved")
        result_df = df.copy(deep=True)
    else:
        pid = utils.extract_pid(file)
        result_df = run_interpolation(df, verbose=verbose, prev_file=prev_file, next_file=next_file, session_file=session_file, sr=sr, gap_threshold=gap_threshold, pid=pid)
    print(result_df.iloc[0,0])
    if "MasterSynced" in file:
        output_file = file.replace('MasterSynced', 'Derived/interpolated_' + str(sr))
    elif "Derived" in file:
        derived_folder_name = utils.extract_derived_folder_name(file)
        output_file = file.replace(derived_folder_name, 'interpolated_' + str(sr))
    if not os.path.exists(os.path.dirname(output_file)):
        os.makedirs(os.path.dirname(output_file))
        
    result_df.to_csv(output_file, index=False, float_format='%.3f')
    if verbose:
        print('Saved interpolated data to ' + output_file)
    return pd.DataFrame()
        
def run_interpolation(df, verbose=True, prev_file=None, next_file=None, session_file=None, sr=None, gap_threshold=1, pid=None, **kwargs):
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

    if prev_file is not None and prev_file != 'None':
        prev_df = pd.read_csv(prev_file, parse_dates=[0], infer_datetime_format=True)
    else:
        prev_df = pd.DataFrame()
    if next_file is not None and next_file != 'None':
        next_df = pd.read_csv(next_file, parse_dates=[0], infer_datetime_format=True)
    else:
        next_df = pd.DataFrame()

    result_df = interpolate(df, verbose=verbose, prev_df=prev_df, next_df=next_df, sr=sr, start_time=start_time, stop_time=stop_time, gap_threshold=gap_threshold)

    return result_df
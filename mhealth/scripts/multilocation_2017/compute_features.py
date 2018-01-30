"""
Script to compute features (based on VM and orientation) used for posture and activity recognition in multilocation paper. 

VM features:
    preprocess: 20Hz butterworth lowpass filter
    features:
        "MEAN"
        'STD'
        'MAX'
        'DOM_FREQ'
        'DOM_FREQ_POWER_RATIO'
        'HIGHEND_FREQ_POWER_RATIO'
        'RANGE'
        'ACTIVE_SAMPLE_PERC'
        'NUMBER_OF_ACTIVATIONS'
        'ACTIVATION_INTERVAL_VAR'

Orientation features:
    preprocess:
        manual orientation fixing
        20Hz butterworth lowpass filter
    features:
        x,y,z median angle
        x,y,z angle range
Usage:
    Production: 
        On all participants
            `mh -r . process multilocation_2017.compute_features --par --pattern MasterSynced/**/Actigraph*.sensor.csv > DerivedCrossParticipants/multilocation_2017.feature.csv`
        On single participant
            `mh -r . -p SPADES_1 process multilocation_2017.compute_features --par --pattern MasterSynced/**/Actigraph*.sensor.csv > SPADES_1/Derived/multilocation_2017.feature.csv`

    Debug:
        `mh -r . -p SPADES_1 process multilocation_2017.compute_features --verbose --pattern MasterSynced/**/Actigraph*.sensor.csv`
"""

import os
import pandas as pd
import numpy as np
import scipy.signal as signal
import mhealth.api.numeric_feature as mnf
import mhealth.api.numeric_transformation as mnt
import mhealth.api.filter as mf
import mhealth.api.windowing as mw
import mhealth.api.utils as mu

def main(file, verbose=True, prev_file=None, next_file=None, session_file="DerivedCrossParticipants/sessions.csv", name='multilocation_2017', ws=12800, ss=12800, threshold=0.2, subwins=4, lowpass_cutoff = 20, **kwargs):
    file = os.path.abspath(file)
    if verbose:
        print("Compute features for " + file)
    df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
    pid = mu.extract_pid(file)
    sid = mu.extract_id(file)
    if verbose:
        print("Prev file is " + str(prev_file))
        print("Next file is " + str(next_file))

    if not os.path.exists(prev_file):
        prev_file = None
    if not os.path.exists(next_file):
        next_file = None

    session_file = os.path.abspath(session_file)
    if session_file is None or pid is None:
        st = df.iloc[0, 0]
        et = df.iloc[df.shape[0]-1, 0]
    else:
        session_df = pd.read_csv(session_file, parse_dates=[0, 1], infer_datetime_format=True)
        selected_sessions = session_df.loc[session_df['pid'] == pid, :]
        if selected_sessions.shape[0] == 0:
            st = df.iloc[0, 0]
            et = df.iloc[df.shape[0]-1, 0]
        else:
            st = selected_sessions.iloc[0, 0]
            et = selected_sessions.iloc[selected_sessions.shape[0] - 1, 1]

    if verbose:
        print('Session start time: ' + str(st))
        print('Session stop time: ' + str(et))

    result_df = run_compute_features(df, verbose=verbose, prev_file=prev_file, next_file=next_file, st=st, et=et, ws=ws, ss=ss, **kwargs)

    if "MasterSynced" in file:
        output_path = file.replace("MasterSynced", "Derived/" + name).replace("sensor", "feature")
    elif "Derived" in file:
        derived_folder_name = utils.extract_derived_folder_name(file)
        output_path = file.replace(derived_folder_name, name).replace('sensor', 'feature')

    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))
        
    result_df.to_csv(output_path, index=False, float_format='%.3f')
    if verbose:
        print('Saved feature data to ' + output_path)

    result_df['pid'] = pid
    result_df['sid'] = sid
    return result_df

def run_compute_features(df, verbose=True, prev_file=None, next_file=None, st=None, et=None, ws=12800, ss=12800, threshold=0.2, subwins=4, lowpass_cutoff = 20, **kwargs):
    # save current file's start and stop time
    chunk_st = df.iloc[0, 0].to_datetime64().astype('datetime64[h]')
    
    if chunk_st < st.to_datetime64():
        chunk_st = st.to_datetime64()
    chunk_et = df.iloc[df.shape[0]-1, 0].to_datetime64().astype('datetime64[h]') + np.timedelta64(1, 'h')
    
    if chunk_et > et.to_datetime64():
        chunk_et = et.to_datetime64()

    if prev_file is not None and prev_file != 'None':
        prev_df = pd.read_csv(prev_file, parse_dates=[0], infer_datetime_format=True)
    else:
        prev_df = pd.DataFrame()
    if next_file is not None and next_file != 'None':
        next_df = pd.read_csv(next_file, parse_dates=[0], infer_datetime_format=True)
    else:
        next_df = pd.DataFrame()

    combined_df = pd.concat([prev_df, df, next_df], axis=0, ignore_index=True)

    sr = mu._sampling_rate(combined_df)
    
    # 20 Hz lowpass filter on vector magnitude data and original data
    vm_data = mnt.vector_magnitude(combined_df.values[:,1:4]).ravel()
    b, a = signal.butter(4, lowpass_cutoff/sr, 'low')
    vm_data_filtered = signal.filtfilt(b, a, vm_data)
    combined_data_filtered = signal.filtfilt(b, a, combined_df.values[:,1:4], axis=0)

    vm_df = pd.DataFrame(data={"HEADER_TIME_STAMP": combined_df.iloc[:,0].values, "VM": vm_data_filtered})
    combined_df.values[:,1:4] = combined_data_filtered 
    def freq_features(X):
        result = mnf.frequency_features(X, sr, freq_range=None, top_n_dominant = 1)
        if len(result) == 0:
            return np.array([np.nan, np.nan, np.nan])
        p1 = result[1]
        pt = result[2]
        ph = result[3]
        p1ratio = p1 / pt
        phratio = ph / pt
        return np.array([p1, p1ratio, phratio])

    vm_features = [
        mnf.mean,
        mnf.std,
        mnf.positive_amplitude,
        freq_features,
        mnf.amplitude_range,
        lambda x: mnf.active_perc(x, threshold),
        lambda x: mnf.activation_count(x, threshold),
        lambda x: mnf.activation_std(x, threshold)
    ]

    vm_feature_names = [
        "MEAN",
        'STD',
        'MAX',
        'DOM_FREQ',
        'DOM_FREQ_POWER_RATIO',
        'HIGHEND_FREQ_POWER_RATIO',
        'RANGE',
        'ACTIVE_SAMPLE_PERC',
        'NUMBER_OF_ACTIVATIONS',
        'ACTIVATION_INTERVAL_VAR'
    ]

    axis_features = [
        lambda x: mnf.accelerometer_orientation_features(x, subwins=subwins)
    ]

    axis_feature_names = [
        "MEDIAN_X_ANGLE",
        "MEDIAN_Y_ANGLE",
        "MEDIAN_Z_ANGLE",
        "RANGE_X_ANGLE",
        "RANGE_Y_ANGLE",
        "RANGE_Z_ANGLE"
    ]

    windows = mw.get_sliding_window_boundaries(start_time=st, stop_time=et, window_duration=ws, step_size=ss)
    chunk_windows_mask = (windows[:,0] >= chunk_st) & (windows[:,0] <= chunk_et)
    chunk_windows = windows[chunk_windows_mask,:]

    vm_feature_df = mw.apply_to_sliding_windows(df=vm_df, sliding_windows=chunk_windows, window_operations=vm_features, operation_names=vm_feature_names, return_dataframe=True)
    axis_feature_df = mw.apply_to_sliding_windows(df=combined_df, sliding_windows=chunk_windows, window_operations=axis_features, operation_names=axis_feature_names, return_dataframe=True)
    feature_df = vm_feature_df.merge(axis_feature_df, on = ['START_TIME', 'STOP_TIME'])

    return feature_df
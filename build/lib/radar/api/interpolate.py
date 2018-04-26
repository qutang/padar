from scipy.interpolate import InterpolatedUnivariateSpline, interp1d
from .date_time import datetime64_to_seconds, seconds_to_datetime64
import numpy as np
import pandas as pd
from .utils import _sampling_rate

def interpolate(df, verbose=True, prev_df=None, next_df=None,  sr=None, start_time=None, stop_time=None, fill_big_gap_with_na=True, gap_threshold = 1, method="spline"):
    """Make timestamps with consistent intervals with interpolation.

    Delete duplicate timestamps, interpolate to make sampling rate consistent with provided interpolation method, default is spline interpolation. Big gaps (more than 1s will not be interpolated)

    Keyword arguments:
        start_time, stop_time -- specified start and end time to be used in the interpolated dataframe. If there are multiple sensor data object from different sensor that may not have exactly the same start time, user can provide one to be used by every one of them. So that it will be easier for feature calculation and data merging later.
        sr -- desired sampling rate
        fill_big_gap_with_na -- whether big gaps should be filled with NaN or just simply not included in the interpolated data frame
        gap_threshold -- time in second to be counted as big gap
        method -- interpolation method, current only support 'spline' and 'linear'
    """
    if verbose:
        print("Original sampling rate: " + str(_sampling_rate(df)))
        
    if sr is None:
        sr = _sampling_rate(df)
    else:
        sr = np.float64(sr)
    if verbose:
        print("New sampling rate: " + str(sr))

    # save current file's start and stop time
    chunk_st = datetime64_to_seconds(df.iloc[0, 0].to_datetime64().astype('datetime64[h]'))
    chunk_et = datetime64_to_seconds(df.iloc[df.shape[0]-1, 0].to_datetime64().astype('datetime64[h]') + np.timedelta64(1, 'h'))

    combined_df = pd.concat([prev_df, df, next_df], axis=0)

    # Drop duplication
    cols = combined_df.columns.values
    combined_df.drop_duplicates(
        subset=cols[0], keep="first", inplace=True)
    
    ts = combined_df.iloc[:,0].values
    # Convert timestamp column to unix numeric timestamps
    ts = datetime64_to_seconds(ts)

    combined_st = ts[0]
    combined_et = ts[-1]

    if start_time is None:
        start_time = ts[0]
    
    if stop_time is None:
        stop_time = ts[-1]

    # make sure st and et are also in unix timestamps
    if type(start_time) != np.float64:
        start_time = datetime64_to_seconds(start_time)
        stop_time = datetime64_to_seconds(stop_time)

    if chunk_st < start_time:
        chunk_st = start_time
    if chunk_et > stop_time:
        chunk_et = stop_time

    chunk_st = seconds_to_datetime64([chunk_st])[0]
    chunk_et = seconds_to_datetime64([chunk_et])[0]

    # make the reference timestamp for interpolation
    ref_ts = np.linspace(start_time, stop_time, np.ceil((stop_time - start_time) * sr))

    # only get the combined_df part
    combined_ref_ts = ref_ts[(ref_ts >= combined_st) & (ref_ts < combined_et)]

    # check whether there are big gaps in the data, we don't interpolate
    # for big gaps!
    big_gap_positions = check_large_gaps(combined_df, ts, gap_threshold = gap_threshold)
    values = combined_df[cols[1:cols.size]].values
    if big_gap_positions.size == 1:
        if verbose:
            print("Use regular interpolation")
        # no big gap then just interpolate regularly
        print(ts.shape)
        print(values.shape)
        print(combined_ref_ts.shape)
        new_ts, new_values = interpolate_regularly(ts, values, combined_ref_ts, sr, method)
    else:
        if verbose:
            print("Use interpolation with big gaps: " + str(big_gap_positions.size))
        # big gaps found, interpolate by chunks
        new_ts, new_values = interpolate_for_big_gaps(big_gap_positions, ts, values, combined_ref_ts, sr, method)

    # Convert the interpolated timestamp column and the reference timestamp
    # column back to datetime
    new_ts = seconds_to_datetime64(new_ts)
    combined_ref_ts = seconds_to_datetime64(combined_ref_ts)
    # make new dataframe
    new_df = pd.DataFrame(
        new_values, columns=cols[1:cols.size], copy=False)
    
    new_df.insert(0, cols[0], new_ts)

    # Fill big gap with NaN if set
    if fill_big_gap_with_na:
        new_df = new_df.set_index(cols[0]).reindex(
            pd.Index(combined_ref_ts, name=cols[0])).reset_index(cols[0])

    # chunk to the original df period
    new_df = new_df.loc[(new_df.iloc[:,0] >= chunk_st) & (new_df.iloc[:,0] < chunk_et),:]

    new_df.iloc[:, 0] = new_df.iloc[:, 0].values.astype('datetime64[ms]')
    return new_df

def interpolate_regularly(ts, values, ref_ts, sr, method):
    new_values = np.apply_along_axis(interpolate_timestamp, axis=0, arr=values, x=ts, new_x=ref_ts, method=method)
    return ref_ts, new_values

def interpolate_for_big_gaps(big_gap_positions, ts, values, ref_ts, sr, method):
    pre_pos = 0
    new_values = np.empty((0, values.shape[1]), float)
    new_ts = np.array([])
    for pos in big_gap_positions:
        # iterate over chunks that are separated by big gaps
        chunk_ts = ts[pre_pos:(pos + 1)]
        chunk_values = values[pre_pos:(pos + 1), :]

        chunk_ref_ts_mask = (ref_ts >= chunk_ts[0]) & (ref_ts <= chunk_ts[-1])
        chunk_ref_ts = ref_ts[chunk_ref_ts_mask]
        if len(chunk_ref_ts) == 0:
            continue
        chunk_new_ts, chunk_new_values = interpolate_regularly(chunk_ts, chunk_values, chunk_ref_ts, sr, method)
        new_values = np.vstack((new_values, chunk_new_values))
        new_ts = np.append(new_ts, chunk_new_ts)
        pre_pos = pos + 1
    return new_ts, new_values

def check_large_gaps(df, x, gap_threshold = 1):
    gaps = np.diff(x)
    '''
    Check big gap positions that are above gap_threshold (in seconds)
    '''
    # max gap is more than 1s, return big gap index positions
    big_gap_positions = np.where(gaps > gap_threshold)[0]
    big_gap_positions = np.append(big_gap_positions, x.size - 1)
    return big_gap_positions

def interpolate_timestamp(y, x, new_x, method='spline'):
    if method == 'spline':
        fitted = InterpolatedUnivariateSpline(x, y)
        new_y = fitted(new_x)
    elif method == 'linear':
        fitted = interp1d(x, y, kind='linear')
        new_y = fitted(new_x)
    return new_y

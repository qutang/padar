"""

functions to divide input data into sliding windows and do computation on each of them

"""

import numpy as np
import pandas as pd
from .date_time import datetime64_to_milliseconds, datetime_to_milliseconds, milliseconds_to_datetime64, datetime

"""
Get start and end time of windows in a 2D numpy array given start and stop time of the whole session

window_duration: duration in milliseconds
step_size: duration in milliseconds
"""

def get_sliding_window_boundaries(start_time, stop_time, window_duration=1000, step_size=None):
	if step_size is None:
		step_size = window_duration

	if isinstance(start_time, pd.Timestamp):
		start_time = pd.to_datetime(start_time)
		stop_time = pd.to_datetime(stop_time)
	
	if isinstance(start_time, np.datetime64):
		st_unix = datetime64_to_milliseconds(start_time)
		et_unix = datetime64_to_milliseconds(stop_time)
	elif isinstance(start_time, datetime):
		st_unix = datetime_to_milliseconds(start_time)
		et_unix = datetime_to_milliseconds(stop_time)
	
	st_unix_windows = np.arange(st_unix, et_unix, step_size)
	et_unix_windows = st_unix_windows + window_duration

	# remove the tailing window that is shorter than the window duration
	mask = et_unix_windows <= et_unix
	st_unix_windows = st_unix_windows[mask]
	et_unix_windows = et_unix_windows[mask]

	st_windows = milliseconds_to_datetime64(st_unix_windows)
	et_windows = milliseconds_to_datetime64(et_unix_windows)

	windows = np.transpose(np.vstack((st_windows, et_windows)))

	return(windows)


def get_sliding_window_dataframe(df, start_time=None, stop_time=None, start_time_col=0, stop_time_col=None):
	if stop_time_col == None:
		stop_time_col = start_time_col
	if start_time == None:
		start_time = df.iloc[0, start_time_col]
		start_time = pd.Timestamp(start_time)
	if stop_time == None:
		# has to get numpy array first, otherwise cannot use -1 indexing
		stop_time = df.iloc[stop_time_col].values[-1]
		stop_time = pd.Timestamp(stop_time)

	if start_time_col == stop_time_col:
		mask = (df.iloc[:,start_time_col] >= start_time) & (df.iloc[:,stop_time_col] < stop_time)
		return df[mask].copy(deep=True)
	else:
		mask = (df.iloc[:,start_time_col] < stop_time) & (df.iloc[:,stop_time_col] > start_time)
		subset_df = df[mask].copy(deep=True)
		subset_df[subset_df.iloc[:, start_time_col] < start_time].iloc[:,start_time_col] = start_time
		subset_df[subset_df.iloc[:, stop_time_col] > stop_time].iloc[:,stop_time_col] = stop_time
		return subset_df

"""
Apply customizable functions to each subset of a dataframe defined by a list of windows' start and end time

df: input dataframe, the first or second column is timestamp to get subset from. E.g., sensor data's dataframe would have the first column to be the start_time_col; annotation data's dataframe would have the second column to be the stop_time_col
sliding_windows: a 2D numpy array with the first column being start time and second column being stop time
window_operations: a list of functions to be applied to the subset of df (in a 2D numpy array)
operation_names: optional a list of functions' output column names corresponding to window_operations
start_time_col: column index for start_time, default to be 0
stop_time_col: column index for stop_time, default to be None, so subsetting will only use start time
"""
def apply_to_sliding_windows(df, sliding_windows, window_operations, operation_names=None, start_time_col=0, stop_time_col=None, send_time_cols=False, return_dataframe=False, empty_row_placeholder=np.nan):
	nrows = sliding_windows.shape[0]
	indices = range(0, nrows)
	output_vectors = []
	ncols_feature = 0
	for i in indices:
		st = sliding_windows[i, 0]
		et = sliding_windows[i, 1]
		chunk = get_sliding_window_dataframe(df, start_time=st, stop_time=et, start_time_col=start_time_col, stop_time_col=stop_time_col)
		if send_time_cols:
			col_names = chunk.columns
			chunk = chunk.values
		else:
			chunk = chunk.drop(chunk.columns[[start_time_col, stop_time_col]], axis=1)
			col_names = chunk.columns
			chunk = chunk.values
		if chunk.shape[0] == 0:
			output_vector = np.array([])
		else:
			outputs = list(map(lambda operation: operation(chunk), window_operations))
			if len(outputs) == 1:
				output_vector = np.array(outputs).ravel()
			else:
				output_vector = np.concatenate(outputs).ravel()
			# get the number of features
			if len(output_vector) > ncols_feature:
				ncols_feature = len(output_vector)
		output_vectors.append(output_vector)
	output_vectors = map(lambda vector: np.repeat(empty_row_placeholder, ncols_feature) if len(vector) == 0 else vector, output_vectors)
	output_matrix = np.stack(output_vectors, axis=0)
	if not return_dataframe:
		return output_matrix
	else:
		output_df = pd.DataFrame(data=output_matrix, columns=operation_names)
		output_df.insert(loc=0, column='START_TIME', value=sliding_windows[:,0])
		output_df.insert(loc=1, column='STOP_TIME', value=sliding_windows[:,1])
		return output_df

def get_synced_time_boundaries(*dfs, start_time_cols=None, stop_time_cols=None, set_rule='union'):
	if start_time_cols is None:
		start_time_cols = np.repeat(0, len(dfs))
	if stop_time_cols is None:
		stop_time_cols = np.repeat(0, len(dfs))

	start_times = [dfs[i].iloc[0, start_time_cols[i]] for i in range(0, len(dfs))]
	stop_times = [dfs[i].iloc[:, stop_time_cols[i]].values[-1] for i in range(0, len(dfs))]

	if set_rule == 'union':
		common_start_time = np.datetime64(np.min(start_times).to_pydatetime())
		common_stop_time = np.max(stop_times)
	elif set_rule == 'intersection':
		common_start_time = np.datetime64(np.max(start_times).to_pydatetime())
		common_stop_time = np.min(stop_times)
	else:
		raise ValueError("Unknown synchronization rule")
	
	# round down (truncate) closest minute for earliest start time
	synced_start_time = common_start_time.astype('datetime64[m]')
	synced_stop_time = common_stop_time.astype('datetime64[m]') + np.timedelta64(1, 'm')

	return (synced_start_time, synced_stop_time)
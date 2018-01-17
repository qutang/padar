import os
import pandas as pd
import numpy as np
import re

def file_lines(file):
	f = open(file, 'rb')
	lines = sum(1 for line in f)
	f.close()
	return lines

def extract_file_type(abspath):
    return os.path.basename(abspath).split('.')[3].lower().strip()

def extract_date(abspath):
        return re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}', os.path.basename(abspath).split('.')[2]).group(0)

def extract_hour(abspath):
	return re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}-([0-9]{2})', os.path.basename(abspath).split('.')[2]).group(1)

def extract_adjacent_file(abspath, side='prev'):
	abspath = os.path.normpath(abspath)
	regex = '[0-9]{4}' + "\\" + os.path.sep + '[0-9]{2}' + "\\" + os.path.sep + '[0-9]{2}' + "\\" + os.path.sep + '[0-9]{2}'
	dh_str = re.search(regex, os.path.dirname(abspath)).group(0)
	ts_str = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{3}', os.path.basename(abspath).split('.')[2]).group(0)
	ts_str_nano = ts_str + "000"
	dh = pd.to_datetime(dh_str, format = '%Y' + os.path.sep + '%m' + os.path.sep + '%d' + os.path.sep + '%H')
	ts = pd.to_datetime(ts_str_nano, format='%Y-%m-%d-%H-%M-%S-%f')
	td = pd.to_timedelta(1, unit='h')
	if side == 'prev':
		dh = dh - td
		ts = ts - td
	elif side == 'next':
		dh = dh + td
		ts = ts + td
	ts_str_adjacent_nano = ts.strftime('%Y-%m-%d-%H-%M-%S-%f')
	ts_str_adjacent = ts_str_adjacent_nano[:-3]
	dh_str_adjacent = dh.strftime('%Y/%m/%d/%H')
	adjacent_file = abspath
	adjacent_file = adjacent_file.replace(dh_str, dh_str_adjacent)
	adjacent_file = adjacent_file.replace(ts_str, ts_str_adjacent)
	return adjacent_file

def extract_id(abspath):
	abspath = os.path.abspath(abspath)
	return os.path.basename(abspath).split('.')[1].split('-')[0].upper().strip()

def extract_derived_folder_name(abspath):
	abspath = os.path.normpath(abspath)
	return abspath.split('Derived')[1].split(os.path.sep)[1]

def extract_pid(abspath):
	if abspath is None:
		return None
	abspath = os.path.normpath(os.path.abspath(abspath))
	if "MasterSynced" in abspath:
		return os.path.basename(os.path.dirname(abspath.split('MasterSynced')[0]))
	elif "Derived" in abspath:
		return os.path.basename(os.path.dirname(abspath.split('Derived')[0]))

def sampling_rate(file):
	df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
	return _sampling_rate(df)

def clip(file, start_time=None, stop_time=None):
	df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
	return clip_dataframe(df, start_time, stop_time)

def clip_dataframe(df, start_time=None, stop_time=None):
	if start_time is None:
		start_time = df.iloc[0, 0].to_datetime64().astype('datetime64[ms]')
	if stop_time is None:
		stop_time = df.iloc[df.shape[0]-1, 0].to_datetime64().astype('datetime64[ms]')

	if type(start_time) is not np.datetime64 or type(stop_time) is not np.datetime64:
		raise ValueError('start_time or stop_time should be in numpy datetime64[ms] format')

	clipped = df.loc[(df.iloc[:,0] >= start_time) & (df.iloc[:, 0] < stop_time),:]
	return clipped

def _sampling_rate(df):
	ts = df.set_index(df.columns[0])
	minute_counts = ts.groupby(pd.TimeGrouper(freq='Min'))[df.columns[1]].count().values
	# drop the first and last window
	minute_counts = minute_counts[1:-1]
	return np.mean(minute_counts / 60)

def sensor_stat(file):
	file_type = extract_file_type(file)
	result = {
			'lines': np.nan,
			'sr': np.nan,
			'max_g': np.nan,
			'min_g': np.nan,
			'max_g_count': np.nan,
			'min_g_count': np.nan
		}
	if file_type != 'sensor':
		return pd.DataFrame(result, index=[0])
	try:
		df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
		result['lines'] = int(df.shape[0])
		result['sr'] = _sampling_rate(df)
		max_value = np.amax(df.values[:,1:])
		result['max_g'] = max_value
		min_value = np.amin(df.values[:,1:])
		result['min_g'] = min_value
		result['max_g_count'] = int(np.sum(df.values[:,1:].flatten() == max_value))
		result['min_g_count'] = int(np.sum(df.values[:,1:].flatten() == min_value))
	except TypeError:
		for key in result:
			if np.isnan(result[key]):
				result[key] = 'TypeError'
	except pd.errors.ParserError:
		for key in result:
			result[key] = 'ParserError'
	return pd.DataFrame(result, index=[0])
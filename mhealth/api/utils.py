import os
import pandas as pd
import numpy as np
import re

def file_lines(file):
	f = open(file, 'rb')
	lines = sum(1 for line in f)
	f.close()
	return lines

def extract_file_type(file):
    return os.path.basename(file).split('.')[3].lower().strip()

def extract_date(file):
        return re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}', os.path.basename(file).split('.')[2]).group(0)

def extract_hour(file):
	return re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}-([0-9]{2})', os.path.basename(file).split('.')[2]).group(1)

def extract_id(sensor_file):
	return os.path.basename(sensor_file).split('.')[1].split('-')[0].upper().strip()

def sampling_rate(file):
	df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
	return _sampling_rate(df)

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
		result['lines'] = df.shape[0]
		result['sr'] = _sampling_rate(df)
		max_value = np.amax(df.values[:,1:])
		result['max_g'] = max_value
		min_value = np.amin(df.values[:,1:])
		result['min_g'] = min_value
		result['max_g_count'] = np.sum(df.values[:,1:].flatten() == max_value)
		result['min_g_count'] = np.sum(df.values[:,1:].flatten() == min_value)
	except TypeError:
		for key in result:
			if np.isnan(result[key]):
				result[key] = 'TypeError'
	except pd.errors.ParserError:
		for key in result:
			result[key] = 'ParserError'
	return pd.DataFrame(result, index=[0])
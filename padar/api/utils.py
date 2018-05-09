import os
import pandas as pd
import numpy as np
import re

def extract_file_type(abspath):
    return os.path.basename(abspath).split('.')[-2].lower().strip()

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

def extract_sensortype(abspath):
	abspath = os.path.abspath(abspath)
	return os.path.basename(abspath).split('.')[0].split('-')[0]

def extract_datatype(abspath):
	abspath = os.path.abspath(abspath)
	if len(os.path.basename(abspath).split('.')[0].split('-')) < 2:
		return ""
	return os.path.basename(abspath).split('.')[0].split('-')[1]

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
	else:
		return os.path.basename(os.path.dirname(abspath))

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
	minute_counts = ts.groupby(pd.TimeGrouper(freq='1S'))[df.columns[1]].count().values
	# drop the first and last window
	minute_counts = minute_counts[1:-1]
	# only choose windows that have more than one samples
	minute_counts = minute_counts[minute_counts > 0]
	return major_element(minute_counts)

def num_of_rows(file):
	with open(file, 'r') as f:
		lines = len(f.readlines())
	return lines

def validate_folder_structure(file):
	file = os.path.normpath(os.path.abspath(file))
	if re.search('MasterSynced\\' + os.path.sep + '[0-9]{4}' + "\\" + os.path.sep + '[0-9]{2}' + "\\" + os.path.sep + '[0-9]{2}' + "\\" + os.path.sep + '[0-9]{2}', file) is not None:
		correct ="True"
	else:
		correct = "False"
	return correct

def validate_filename(file):
	filename = os.path.basename(file)
	pattern = '([A-Za-z0-9]+\-){1,2}[A-Za-z0-9]+\.[A-Za-z0-9]+\-[A-Za-z0-9]+\.[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{3}-[MP]{1}[0-9]{4}\.[a-z]+\.csv'
	if re.search(pattern, file) is not None:
		return "True"
	else:
		return "False"

def validate_csv_header(file):
	file_type = extract_file_type(os.path.abspath(file))
	with open(file, 'r') as f:
		header = f.readline().strip()
	tokens = header.split(',')
	if len(tokens) < 2:
		return "Header has less than 2 columns"
	message = ""
	if tokens[0] != 'HEADER_TIME_STAMP':
		message = message + "HEADER_TIME_STAMP|"
	if file_type == 'annotation' or file_type == 'event':
		if tokens[1] != 'START_TIME':
			message = message + 'START_TIME|'
		if tokens[2] != 'STOP_TIME':
			message = message + 'STOP_TIME|'
	if file_type == 'annotation':
		if tokens[3] != 'LABEL_NAME':
			message = message + 'LABEL_NAME'
	if len(message) > 0:
		return message
	else:
		return "True"

def na_rows(file):
	file_type = extract_file_type(file)
	if file_type == 'sensor':
		df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
	elif file_type == 'annotation' or file_type == 'event':
		df = pd.read_csv(file, parse_dates=[0,1,2], infer_datetime_format=True)
	return df.shape[0] - df.dropna().shape[0]

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

def major_element(ls):
	(values,counts) = np.unique(ls,return_counts=True)
	ind=np.argmax(counts)
	return values[ind]

def generate_output_filepath(file, setname, newtype=None, datatype=None):
	file = os.path.normpath(os.path.abspath(file))
	if "MasterSynced" in file:
		new_file = file.replace('MasterSynced', 'Derived' + os.path.sep + setname)
	elif "Derived" in file:
		new_file = file.replace(extract_derived_folder_name(file), setname)
	else:
		new_file = file.replace(os.path.basename(file), 'Derived' + os.path.sep + setname + os.path.sep + os.path.basename(file))

	if newtype is not None:
		new_file = new_file.replace(extract_file_type(file), newtype)
	if datatype is not None:
		new_file = new_file.replace(extract_datatype(file), datatype)
	
	return new_file

def get_st_et(data, pid, session_file=None, st_col=0, et_col=0):
	if session_file is None or session_file == "None" or pid is None:
		st = np.min(data.iloc[:, st_col])
		et = np.max(data.iloc[:, et_col])
	else:
		session_file = os.path.abspath(session_file)
		session_df = pd.read_csv(session_file, parse_dates=[0, 1], infer_datetime_format=True)
		selected_sessions = session_df.loc[session_df['pid'] == pid, :]
		if selected_sessions.shape[0] == 0:
			st = np.min(selected_sessions.iloc[:, st_col])
			et = np.max(selected_sessions.iloc[:, et_col])
		else:
			st = np.min(selected_sessions.iloc[:, 0])
			et = np.max(selected_sessions.iloc[:, 1])
	
	return st, et

def get_sid_from_location(pid, location, location_mapping_file):
	location_mapping_file = os.path.abspath(location_mapping_file)
	if location_mapping_file is None or location is None or pid is None:
		return None
	else:
		location_mapping_df = pd.read_csv(location_mapping_file)
		selected_mapping = location_mapping_df.loc[(location_mapping_df['PID'] == pid) & (location_mapping_df['LOCATION'] == location), 'SENSOR_ID']
		if selected_mapping.shape[0] == 0:
			return None
		return selected_mapping.values[0]

def get_location_from_sid(pid, sid, location_mapping_file):
	if location_mapping_file is None or location_mapping_file == 'None' or sid is None or pid is None:
		return None
	else:
		location_mapping_file = os.path.abspath(location_mapping_file)
		location_mapping_df = pd.read_csv(location_mapping_file)
		selected_mapping = location_mapping_df.loc[(location_mapping_df['PID'] == pid) & (location_mapping_df['SENSOR_ID'] == sid), 'LOCATION']
		if selected_mapping.shape[0] == 0:
			return "unknown"
		return selected_mapping.values[0]
from glob import glob
import os
from itertools import islice
import re
from ..utils import extract_pid
import pandas as pd

def parse_location_mapping(folder, location_pattern, pid_pattern):
	pattern = os.path.join(os.path.abspath(folder), '*RAW.csv')
	actigraph_raw_csvs = glob(pattern, recursive=True)
	location_mapping_list = [_parse_location_mapping(raw_csv, location_pattern, pid_pattern) for raw_csv in actigraph_raw_csvs]
	location_mapping = pd.concat(location_mapping_list)
	location_mapping.reset_index(drop=True, inplace=True)
	return location_mapping
	
def _parse_location_mapping(filepath, location_pattern, pid_pattern):
	filepath = os.path.abspath(filepath)
	matches = re.search(pid_pattern, filepath)
	pid = matches.group(1)
	matches = re.search(location_pattern, os.path.basename(filepath))
	loc = matches.group(1)
	with open(filepath, 'r') as f:
		headers = list(islice(f, 2))
		matches = re.search('Serial Number: ([A-Z0-9]+)', headers[1])
		sn = matches.group(1)
	result = pd.DataFrame(data={'PID': [pid], 'SENSOR_ID': [sn], 'LOCATION': [loc]})
	result = result[['PID', 'SENSOR_ID', 'LOCATION']]
	return result
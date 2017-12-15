"""
Script to run on a sensor data file for clipping
"""

import os
import pandas as pd
import mhealth.api as mh

def main(file, verbose=True, start_time=None, stop_time=None):
	file = os.path.abspath(file)
	if start_time is not None:
		st = pd.to_datetime(start_time, infer_datetime_format=True).to_datetime64().astype('datetime64[ms]')
	else:
		st = start_time
	if stop_time is not None:
		et = pd.to_datetime(stop_time, infer_datetime_format=True).to_datetime64().astype('datetime64[ms]')
	else:
		et = stop_time
	clipped = mh.clip(file, start_time=st, stop_time=et)
	clipped['pid'] = mh.extract_pid(file)
	clipped['id'] = mh.extract_id(file)
	clipped['date'] = mh.extract_date(file)
	clipped['hour'] = mh.extract_hour(file)
	return clipped
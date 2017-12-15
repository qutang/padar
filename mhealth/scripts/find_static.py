"""
Script to run on a sensor data file for clipping
"""

import os
import pandas as pd
import mhealth.api as mh

def main(file, verbose=True, angle_diff=30):
	file = os.path.abspath(file)
	df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
	static_chunks = mh.Calibrator(df, angle_diff=angle_diff).find().static
	static_chunks['pid'] = mh.extract_pid(file)
	static_chunks['id'] = mh.extract_id(file)
	static_chunks['date'] = mh.extract_date(file)
	static_chunks['hour'] = mh.extract_hour(file)
	return static_chunks
"""
Script to run on a sensor data file to calibrate raw acceleration data
"""

import os
import pandas as pd
import mhealth.api as mh
import mhealth.api.utils as utils

def main(file, verbose=True, static_chunk_file=None):
	file = os.path.abspath(file)
	if verbose:
		print("Process " + file)
	df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
	if static_chunk_file is None:
		raise ValueError("You must provide a static_chunk_file")
	static_chunk_file = os.path.abspath(static_chunk_file)
	static_chunks = pd.read_csv(static_chunk_file, parse_dates=[0], infer_datetime_format=True)
	sid = utils.extract_id(file)
	pid = utils.extract_pid(file)
	selected_static_chunks = static_chunks.loc[(static_chunks['id'] == sid) & (static_chunks['pid'] == pid),:]
	chunk_count = selected_static_chunks.groupby(['WINDOW_ID', 'COUNT', 'date', 'hour']).count().shape[0]
	if verbose:
		print("Found " + str(chunk_count) + " static chunks")
	if chunk_count < 9:
		print("Need at least 9 static chunks for calibration, skip and use original data")
		calibrated_df = df
	else:
		calibrated_df = mh.Calibrator(df, max_points=100).set_static(selected_static_chunks).run(verbose=verbose).calibrated
	output_file = file.replace('MasterSynced', 'Derived/calibrated')
	if not os.path.exists(os.path.dirname(output_file)):
		os.makedirs(os.path.dirname(output_file))
	calibrated_df.to_csv(output_file, index=False, float_format='%.3f')
	if verbose:
		print('Saved calibrated data to ' + output_file)
	return pd.DataFrame()
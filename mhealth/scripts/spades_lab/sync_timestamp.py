"""
script to synchronize timestamps for Actigraph sensors in spades lab dataset based on the offset_mapping.csv file in DerivedCrossParticipants folder
"""

import os
import pandas as pd
import numpy as np
import mhealth.api.utils as utils

def main(file, verbose=True, sync_file=None, **kwargs):
	file = os.path.abspath(file)
	if verbose:
		print("Process " + file)
	df = pd.read_csv(file, parse_dates=[0], infer_datetime_format=True)
	pid = utils.extract_pid(file)
	pid = int(pid.split("_")[1])

def run_sync_timestamp(df, verbose=True, sync_file=None, pid=None):
	result = df.copy(deep=True)
	if pid is None:
		raise ValueError("You must provide a valid pid")
	if sync_file is not None:
		sync_file = os.path.abspath(sync_file)
		offset_mapping = pd.read_csv(sync_file)
		selected_offset = offset_mapping.loc[offset_mapping['PID'] == pid,:]
		offset = selected_offset.iloc[0,1]
	else:
		offset = 0
	if verbose:
		print("Offset is: " + str(offset) + " seconds")
	
	result.iloc[:,0] = result.iloc[:,0] + pd.to_timedelta(offset, unit='s')
	return result
	
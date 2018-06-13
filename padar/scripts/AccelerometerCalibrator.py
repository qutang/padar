"""
Script to use the static chunks found by `StaticFinder` to calibrate accelerometer raw data so that gravity drift is minimized. 

Prerequiste:
	Run `StaticFinder` before using this script

Usage:
	pad -p <PID> -r <root> process -p <PATTERN> --par AccelerometerCalibrator <options>

	options:
	
		--static_chunks <path>: the filepath (relative to root folder or absolute path) that contains the static chunks found by `StaticFinder`. User must provide this information in order to use the script.
		
		--output_folder <folder name>: the folder name that the script will save calibrated data to in a participant's Derived folder. User must provide this information in order to use the script.
		
	output:
		The command will not print any output to console. The command will save the calibrated hourly files to the <output_folder>

Examples:

	1.  Calibrate the Actigraph raw data files for participant SPADES_1 in parallel and save it to a folder named 'Calibrated' in the 'Derived' folder of SPADES_1
	
    	pad -p SPADES_1 process AccelerometerCalibrator --par -p MasterSynced/**/Actigraph*.sensor.csv --output_folder Calibrated --static_chunks SPADES_1/Derived/static_chunks.csv

	2. Calibrate the Actigraph raw data files for all participants in a dataset in parallel and save it to a folder named 'Calibrated' in the 'Derived' folder of each participant

			pad process AccelerometerCalibrator --par -p MasterSynced/**/Actigraph*.sensor.csv -output_folder Calibrated --static_chunks DerivedCrossParticipants/static_chunks.csv
"""

import os
import pandas as pd
from .. import api as mhapi
from ..api import utils as mu
from .BaseProcessor import SensorProcessor
from ..utility import logger

def build(**kwargs):
	return AccelerometerCalibrator(**kwargs).run_on_file

class AccelerometerCalibrator(SensorProcessor):
	def __init__(self, verbose=True, independent=True, violate=False, static_chunks=None, output_folder=None):
		SensorProcessor.__init__(self, verbose=verbose, independent=independent, violate=violate)
		self.name = 'AccelerometerCalibrator'
		if static_chunks is None:
			logger.error('<--static_chunks> option must be provided')
			exit(1)
		if output_folder is None:
			logger.error('<--output_folder> option must be provided')
			exit(1)
		self.static_chunks = static_chunks
		self.output_folder = output_folder

	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		pid = self.meta['pid']
		sid = self.meta['sid']
		static_chunks = os.path.abspath(self.static_chunks)
		static_chunks = pd.read_csv(self.static_chunks, parse_dates=[0], infer_datetime_format=True)
		if self.violate:
			selected_static_chunks = static_chunks.loc[static_chunks['pid'] == pid,:]
		else:
			selected_static_chunks = static_chunks.loc[(static_chunks['id'] == sid) & (static_chunks['pid'] == pid),:]
		chunk_count = selected_static_chunks.groupby(['WINDOW_ID', 'COUNT', 'date', 'hour']).count().shape[0]
		if self.verbose:
			logger.info("Found " + str(chunk_count) + " static chunks")
		if chunk_count < 9:
			logger.warn("Need at least 9 static chunks for calibration, skip and use original data")
			calibrated_df = combined_data
		else:
			calibrated_df = mhapi.Calibrator(combined_data, max_points=100, verbose=self.verbose).set_static(selected_static_chunks).run().calibrated
		return calibrated_df

	def _post_process(self, result_data):
		output_file = mu.generate_output_filepath(self.file, setname=self.output_folder, newtype='sensor')
		if not os.path.exists(os.path.dirname(output_file)):
			os.makedirs(os.path.dirname(output_file))
		result_data.to_csv(output_file, index=False, float_format='%.9f')
		if self.verbose:
			logger.info('Saved calibrated data to ' + output_file)
		return pd.DataFrame()
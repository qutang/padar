"""
Script to run on a sensor data file to calibrate raw acceleration data

Usage:
	Production
        `mh -r . process --par --verbose --pattern SPADES_*/MasterSynced/**/Actigraph*.sensor.csv AccelerometerCalibrator --static_chunk_file DerivedCrossParticipants/static_chunks.csv --setname Calibrated`
        `mh -r . -p SPADES_1 process --par --verbose --pattern MasterSynced/**/Actigraph*.sensor.csv AccelerometerCalibrator --static_chunk_file DerivedCrossParticipants/static_chunks.csv --setname Calibrated`

    Debug
         `mh -r . -p SPADES_1 process --verbose --pattern MasterSynced/**/Actigraph*.sensor.csv AccelerometerCalibrator --static_chunk_file DerivedCrossParticipants/static_chunks.csv --setname Calibrated`
"""

import os
import pandas as pd
from .. import api as mhapi
from ..api import utils as mu
from .BaseProcessor import SensorProcessor

def build(**kwargs):
	return AccelerometerCalibrator(**kwargs).run_on_file

class AccelerometerCalibrator(SensorProcessor):
	def __init__(self, verbose=True, independent=True, static_chunk_file=None, setname='Calibrated'):
		SensorProcessor.__init__(self, verbose=verbose, independent=independent)
		self.name = 'AccelerometerCalibrator'
		self.static_chunk_file = static_chunk_file
		self.setname = setname

	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		if self.static_chunk_file is None:
			raise ValueError("You must provide a static_chunk_file")
		if self.meta['pid'] is None:
			raise ValueError("You must provide a valid pid")
		if self.meta['sid'] is None:
			raise ValueError("You must provide a valid sensor id")
		pid = self.meta['pid']
		sid = self.meta['sid']
		static_chunk_file = os.path.abspath(self.static_chunk_file)
		static_chunks = pd.read_csv(self.static_chunk_file, parse_dates=[0], infer_datetime_format=True)
		selected_static_chunks = static_chunks.loc[(static_chunks['id'] == sid) & (static_chunks['pid'] == pid),:]
		chunk_count = selected_static_chunks.groupby(['WINDOW_ID', 'COUNT', 'date', 'hour']).count().shape[0]
		if self.verbose:
			print("Found " + str(chunk_count) + " static chunks")
		if chunk_count < 9:
			print("Need at least 9 static chunks for calibration, skip and use original data")
			calibrated_df = combined_data
		else:
			calibrated_df = mhapi.Calibrator(combined_data, max_points=100).set_static(selected_static_chunks).run(verbose=self.verbose).calibrated
		return calibrated_df

	def _post_process(self, result_data):
		output_file = mu.generate_output_filepath(self.file, setname=self.setname, newtype='sensor')
		if not os.path.exists(os.path.dirname(output_file)):
			os.makedirs(os.path.dirname(output_file))
		result_data.to_csv(output_file, index=False, float_format='%.3f')
		if self.verbose:
			print('Saved calibrated data to ' + output_file)
		return pd.DataFrame()
"""
Script to find at most 12 one second static (< 0.01g) chunks (windows) that have at least 30 degree difference in its orientation for each data file for sensor calibration

Usage:
	pad -p <PID> -r <root> process -p <PATTERN> --par -o <OUTPUT_FILEPATH> StaticFinder <options>

	options:
		--angle_diff <degree>: default is 30 degrees, the difference of orientation between static chunks

	output:
		The command will print the founded static chunks (in a pandas dataframe) in csv format to standard output console, in following columns

		HEADER_TIME_STAMP,X,Y,Z,WINDOW_ID,COUNT,pid,id,date,hour
		...

		WINDOW_ID: the current count of current static chunk window relative to the original raw data file
		COUNT: the current count of current static chunk window relative to all static chunks
		pid: the participant's id
		id: the sensor's id
		date: current date
		hour: current hour

Examples:

	1. Find static chunks for the Actigraph raw data files for participant SPADES_1 in parallel and save it to the Derived folder of this participant
	
    	pad -p SPADES_1 process StaticFinder --par -p MasterSynced/**/Actigraph*.sensor.csv -o Derived/static_chunks.csv

	2. Find static chunks for the Actigraph raw data files for all participants in a dataset in parallel and save it to the DerivedCrossParticipants folder of the dataset

		pad process StaticFinder --par -p MasterSynced/**/Actigraph*.sensor.csv -o DerivedCrossParticipants/static_chunks.csv
"""

from .. import api as mhapi
from .BaseProcessor import SensorProcessor

def build(**kwargs):
	return StaticFinder(**kwargs).run_on_file

class StaticFinder(SensorProcessor):
	def __init__(self, verbose=True, independent=True, violate=False, angle_diff=30):
		SensorProcessor.__init__(self, verbose=verbose, independent=independent, violate=violate)
		self.angle_diff = 30
		self.name = 'StaticFinder'

	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		static_chunks = mhapi.Calibrator(combined_data, angle_diff=self.angle_diff, verbose=self.verbose).find().static
		return static_chunks

	def _post_process(self, result_data):
		result_data['pid'] = self.meta['pid']
		result_data['id'] = self.meta['sid']
		result_data['date'] = self.meta['date']
		result_data['hour'] = self.meta['hour']
		return result_data
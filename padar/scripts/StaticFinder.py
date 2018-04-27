"""
Script to run on a sensor data file to find static chunks for calibration
Usage:
    mh -r . process StaticFinder --par --pattern SPADES_*/MasterSynced/**/Actigraph*.sensor.csv > DerivedCrossParticipants/static_chunks.csv
"""

from .. import api as mhapi
from .BaseProcessor import SensorProcessor

def build(**kwargs):
	return StaticFinder(**kwargs).run_on_file

class StaticFinder(SensorProcessor):
	def __init__(self, verbose=True, independent=True, angle_diff=30):
		SensorProcessor.__init__(self, verbose=verbose, independent=independent)
		self.angle_diff = 30
		self.name = 'StaticFinder'

	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		static_chunks = mhapi.Calibrator(combined_data, angle_diff=self.angle_diff).find().static
		return static_chunks

	def _post_process(self, result_data):
		result_data['pid'] = self.meta['pid']
		result_data['id'] = self.meta['sid']
		result_data['date'] = self.meta['date']
		result_data['hour'] = self.meta['hour']
		return result_data
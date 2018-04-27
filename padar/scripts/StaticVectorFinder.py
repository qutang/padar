"""
Script to run on a sensor data file to find static vectors
Usage:
    mh -r . process StaticFinder --par --pattern SPADES_*/MasterSynced/**/Actigraph*.sensor.csv > DerivedCrossParticipants/static_chunks.csv

Debug:
	mh -r . -p SPADES_1 process StaticVectorFinder --par --pattern MasterSynced/**/Actigraph*.sensor.csv > SPADES_1/Derived/static_vectors.csv
"""

from .. import api as mhapi
from .BaseProcessor import SensorProcessor

def build(**kwargs):
	return StaticVectorFinder(**kwargs).run_on_file

class StaticVectorFinder(SensorProcessor):
	def __init__(self, verbose=True, independent=True):
		SensorProcessor.__init__(self, verbose=verbose, independent=independent)
		self.name = 'StaticVectorFinder'

	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		static_chunks = mhapi.StaticFinder(combined_data).find().static
		return static_chunks

	def _post_process(self, result_data):
		result_data['pid'] = self.meta['pid']
		result_data['id'] = self.meta['sid']
		return result_data
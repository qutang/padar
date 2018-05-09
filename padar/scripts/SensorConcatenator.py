"""
Script simply copy over a file to the new location while keep the mhealth folder structure

Usage:
	mh -r . process Copier --verbose --pattern SPADES_*/MasterSynced/**/*.annotation.csv --setname Preprocessed
"""

from .BaseProcessor import SensorProcessor
from ..api import utils as mu
import os
import shutil
import pandas as pd

def build(**kwargs):
	return SensorConcatenator(**kwargs).run_on_file

class SensorConcatenator(SensorProcessor):
	def __init__(self, verbose=True, independent=True, violate=False, location_mapping_file=None):
		SensorProcessor.__init__(self, verbose=verbose, independent=independent, violate=violate)
		self.name = 'SensorConcatenator'
		self.location_mapping_file = location_mapping_file

	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		# combined_data is actually the filename
		return combined_data

	def _post_process(self, result_data):
		if self.violate:
			return result_data
		else:
			location = mu.get_location_from_sid(self.meta['pid'], self.meta['sid'], self.location_mapping_file)
			result_data['pid'] = self.meta['pid']
			result_data['sid'] = self.meta['sid']
			result_data['location'] = location
			return result_data
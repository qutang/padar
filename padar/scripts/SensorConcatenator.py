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
	def __init__(self, verbose=True, independent=True, violate=False):
		SensorProcessor.__init__(self, verbose=verbose, independent=independent, violate=violate)
		self.name = 'SensorConcatenator'

	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		# combined_data is actually the filename
		return combined_data

	def _post_process(self, result_data):
		return result_data
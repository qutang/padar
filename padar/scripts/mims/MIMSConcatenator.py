"""
Script simply copy over a file to the new location while keep the mhealth folder structure

Usage:
	mh -r . process mims.MIMSConcatenator --pattern MasterSynced/**/day_by_day_mims.csv > DerivedCrossParticipants/day_by_day_mims.csv
"""

from ..BaseProcessor import SensorProcessor
from ..api import utils as mu
import os
import shutil
import pandas as pd

def build(**kwargs):
	return MIMSConcatenator(**kwargs).run_on_file

class MIMSConcatenator(SensorProcessor):
	def __init__(self, verbose=True, independent=True, violate=True, setname='Concatenated'):
		SensorProcessor.__init__(self, verbose=verbose, violate=violate, independent=independent)
		self.setname = setname
		self.name = 'MIMSConcatenator'

	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		# combined_data is actually the filename
		return combined_data

	def _post_process(self, result_data):
		result_data['pid'] = self.meta['pid']
		return result_data
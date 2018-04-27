"""
Script simply copy over a file to the new location while keep the mhealth folder structure

Usage:
	mh -r . process Copier --verbose --pattern SPADES_*/MasterSynced/**/*.annotation.csv --setname Preprocessed
"""

from .BaseProcessor import Processor
from ..api import utils as mu
import os
import shutil
import pandas as pd

def build(**kwargs):
	return Copier(**kwargs).run_on_file

class Copier(Processor):
	def __init__(self, verbose=True, independent=True, setname='Copied'):
		Processor.__init__(self, verbose=verbose, independent=independent)
		self.setname = setname
		self.name = 'Copier'

	def _load_file(self, file, prev_file=None, next_file=None):
		file = os.path.normpath(os.path.abspath(file))
		return file, prev_file, next_file

	def _merge_data(self, data, prev_data=None, next_data=None):
		return data, None, None

	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		# combined_data is actually the filename
		return combined_data

	def _post_process(self, result_data):
		# result_data is actually the filename
		output_file = mu.generate_output_filepath(self.file, self.setname)
		if not os.path.exists(os.path.dirname(output_file)):
			os.makedirs(os.path.dirname(output_file))
		shutil.copyfile(self.file, output_file)
		if self.verbose:
			print("Copied file to " + output_file)
		return pd.DataFrame()
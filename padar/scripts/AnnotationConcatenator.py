"""
Script simply copy over a file to the new location while keep the mhealth folder structure

Usage:
	mh -r . process Copier --verbose --pattern SPADES_*/MasterSynced/**/*.annotation.csv --setname Preprocessed
"""

from .BaseProcessor import AnnotationProcessor
from ..api import utils as mu
import os
import shutil
import pandas as pd

def build(**kwargs):
	return AnnotationConcatenator(**kwargs).run_on_file

class AnnotationConcatenator(AnnotationProcessor):
	def __init__(self, verbose=True, independent=True, violate=False):
		AnnotationProcessor.__init__(self, verbose=verbose, independent=independent, violate=violate)
		self.name = 'AnnotationConcatenator'

	def _run_on_data(self, combined_data, data_start_indicator, data_stop_indicator):
		# combined_data is actually the filename
		return combined_data

	def _post_process(self, result_data):
		result_data['protocol'] = self.meta['sensor_type']
		result_data['annotator'] = self.meta['sid']
		result_data['pid'] = self.meta['pid']
		return result_data